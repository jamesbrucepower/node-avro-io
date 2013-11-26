var libpath 	= process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/';

var fs 			= require('fs');
var util 		= require('util');
var zlib 		= require('zlib');
var snappy 		= require('snappy');
var crc32 		= require('buffer-crc32');
var _ 			= require('underscore');
var Stream 		= require('stream').Stream;
var Transform   = require('stream').Transform;
var IO 			= require(libpath + 'io');
var Avro 		= require(libpath + 'schema');
var AvroErrors 	= require(libpath + 'errors');

// Constants
var VERSION = 1;
var SYNC_SIZE = 16;
var DEFAULT_BUFFER_SIZE = 8192;
var VALID_CODECS = ["null", "deflate", "snappy"];

function magic() {
    return "Obj" + String.fromCharCode(VERSION);
};

function metaSchema() {
    return Avro.Schema({
        "type": "record",
        "name": "org.apache.avro.file.Header",
        "fields" : [
            {
                "name": "magic",
                "type": {
                    "type": "fixed",
                    "name": "magic",
                    "size": magic().length
                }
            },
            {
                "name": "meta",
                "type": {
                    "type": "map",
                    "values": "string"
                }
            },
            {
                "name": "sync",
                "type": {
                    "type": "fixed",
                    "name": "sync",
                    "size": SYNC_SIZE
                }
            }
        ]
    });
};

function blockSchema() {
    return Avro.Schema({
        "type": "record", "name": "org.apache.avro.block",
        "fields" : [
            {"name": "objectCount", "type": "long" },
            {"name": "objects", "type": "bytes" },
            {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": SYNC_SIZE}}
        ]
    });
};

// AvroFile Class
var AvroFile = function() {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();

    var _operation;

    // Public methods
    this.open = function(path, schema, options) {
        var _options = _.extend({
            codec:      "null",
            flags:      'r',
            encoding:   null,
            mode:       0666,
            bufferSize: 64 * 1024
        }, options);

        switch (_options.flags) {
            case "r":
                _operation = new Reader(schema);
                var fileStream = fs.createReadStream(path, _options);
                fileStream.pipe(_operation);
                break;
            case "w":
                var fileStream = fs.createWriteStream(path, _options);
                _operation = new Writer(schema, _options.codec);
                _operation.pipe(fileStream);
                break;
            default:
                throw new AvroErrors.FileError("Unsupported operation %s on file", _options.flags);
        }
        return _operation;
    };

}

function Block(size) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(size);

    size = size || 0;
    this._writeOffset = 0;
    this._readOffset = 0;
    this._buffer = new Buffer(size);
    this.reUseBuffer = true;
}

Block.prototype.__defineGetter__('length', function () {
  return this._writeOffset;
});

Block.prototype.__defineGetter__('offset', function () {
  return this._readOffset;
});

Block.prototype.__defineGetter__('remainingBytes', function() {
    return this._writeOffset - this._readOffset;
});

_.extend(Block.prototype, {

    flush: function() {
        this._writeOffset = this._readOffset = 0;
    },

    rewind: function() {
        this._readOffset = 0;
    },

    _bufferSize: function(size) {
        if (!this._buffer.length) {
            return size;
        } else if (this._buffer.length < DEFAULT_BUFFER_SIZE) {
            var doubleSize = this._buffer.length * 2;
            return (doubleSize - this._buffer.length) > size ? doubleSize : this._buffer.length + size;
        } else {
            return this._writeOffset + size;
        }
    },

    _canReUseBuffer: function(size) {
        return this.reUseBuffer && this._readOffset >= size;
    },

    _resizeIfRequired: function(size) {
        if (this._canReUseBuffer(size)) {
            if (this._readOffset != this._writeOffset)
                this._buffer.copy(this._buffer, 0, this._readOffset, this._writeOffset);
            this._writeOffset = this.remainingBytes;
            this._readOffset = 0;
        } else if (this._writeOffset + size > this._buffer.length) {
            var oldBuffer = this._buffer;
            this._buffer = new Buffer(this._bufferSize(size));
            oldBuffer.copy(this._buffer, 0);
            oldBuffer = null;
        }
    },

    skip: function(size) {
        if ((this._readOffset + size) >= 0 && size <= this.remainingBytes)
            this._readOffset += size;
        else
            throw new AvroErrors.BlockError('tried to skip(%d) outsite of the block(%d) at %d', size, this.remainingBytes, this._readOffset);
    },

    read: function(size) {
        var self = this;
        if (size > this.remainingBytes) {
            return new AvroErrors.BlockDelayReadError('tried to read %d bytes past the amount written to the block with remaining bytes %d at read offset %d',
                                      size, this.remainingBytes, this._readOffset);
        } else if (this._readOffset + size > this._buffer.length) {
            throw new AvroErrors.BlockError('tried to read %d bytes outside of the buffer(%d) at read offset %d',
                                     size, this._buffer.length, this._readOffset);
        } else if (size === 1) {
            return this._buffer[this._readOffset++];
        } else if (size < 0) {
            throw new AvroErrors.BlockError("Tried to read a negative amount of %d bytes", size);
        } else {
            this._readOffset += size;
            return this._buffer.slice(this._readOffset - size, this._readOffset);
        }
    },

    // write() supports an array of numbers of a Buffer
    write: function(value) {
        var len = (Buffer.isBuffer(value) || _.isArray(value)) ? value.length : 1;
        this._resizeIfRequired(len);

        if (Buffer.isBuffer(value)) {
            value.copy(this._buffer, this._writeOffset);
            this._writeOffset += value.length;
        } else if (_.isArray(value)) {
            var item;
            // TODO: items in array could be an object
            while (item = value.shift()) {
                this._buffer[this._writeOffset++] = item;
            }
        } else {
            this._buffer[this._writeOffset++] = value;
        }
    },

    isEqual: function(value) {
        if (Buffer.isBuffer(value) || _.isArray(value)) {
            for (var i = 0; i < value.length; i++) {
                if (this._buffer[i] !== value[i])
                    return false;
            }
        } else {
            throw new AvroErrors.BlockError("must supply an array or buffer");
        }
        return true;
    },

    slice: function(start, end) {
        start = start || 0;
        end = end || this._writeOffset;
        return this._buffer.slice(start, end);
    },

    // Convert a Block to a Buffer
    toBuffer: function() {
        return this.slice();
    },

    // Show a string representation of the Block
    toString: function() {
        return "Block: " + util.inspect(this.slice());
    }, 

    inpect: function() {
        return this.toString();
    }
});

// Reader Class
function Reader(options) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(options);

    options = options || {}
    options.objectMode = true;
    Transform.call(this, options);
    
    this._fileBlock = new Block();
    this._datumBlock = new Block();
    this._inBody = false;
    this.header = null;
    this.decoder = options.decoder || IO.BinaryDecoder(this._fileBlock);
    this.datumReader = IO.DatumReader(null, options.schema);
}

util.inherits(Reader, Transform);

_.extend(Reader.prototype, {

    _snappyDecompress: function(rawData, callback) {
        var compressedData = rawData.slice(0, rawData.length - 4);
        var checksum = rawData.slice(rawData.length - 4, rawData.length);
        snappy.decompress(compressedData, snappy.parsers.raw, function(err, data) {
            if (err) return callback(err);
            var calculatedChecksum = crc32(data);
            if (calculatedChecksum.readUInt32BE(0) !== checksum.readUInt32BE(0))
                callback(new AvroErrors.FileError("Failed checksum from decompressed snappy data block %d !== %d",
                    calculatedChecksum.readUInt32BE(0), checksum.readUInt32BE(0)));
            else
                callback(null, data);
        });
    },

    decompressData: function(data, codec, callback) {
        switch(codec) {
			case "null":    callback(null, data);                   break;
            case "deflate": zlib.inflateRaw(data, callback);        break;
            case "snappy":  this._snappyDecompress(data, callback); break;
            default:
                callback(new AvroErrors.FileError("Unsupported codec %s", codec));
                break;
        }
    },

    _readHeader: function() {
        var self = this;
        var header = this.datumReader.readData(metaSchema(), null, this.decoder);
        if (header instanceof Error) {
            if (header instanceof AvroErrors.BlockDelayReadError)
                return header;
            else
                this.emit('error', header);
        } else if (header.magic.toString() !== magic()) {
            this.emit('error', new AvroErrors.FileError("Not an avro file, header was %j", header));
        }
        try {
            var schema = JSON.parse(header.meta['avro.schema'].toString());
        } catch(e) {
            schema = header.meta['avro.schema'].toString();
        }
        this.writersSchema = Avro.Schema(schema);
        this.datumReader.writersSchema = this.writersSchema;
        this.header = header;
        this.header.meta['avro.schema'] = schema;
        this.emit('header', header);

        return this._fileBlock.offset;
    },

    _readBlock: function(cb) {

        var self = this;
        this.decoder.input(this._fileBlock);
        var block = this.datumReader.readData(blockSchema(), null, this.decoder);

        if (block instanceof AvroErrors.BlockDelayReadError) {
            cb(block);
        } else {
            // If the sync marker doesn't match, maybe there is no sync marker, try skipping back
            if (block.sync && block.sync.toString() !== this.header.sync.toString())
                self._fileBlock.skip(-SYNC_SIZE);

            var codec = this.header.meta['avro.codec'].toString();
            this.decompressData(block.objects, codec, function(err, data) {
                if (err) 
					self.emit('error',  err);
                else {
					if (data) {
						self._datumBlock.write(data);
						self.decoder.input(self._datumBlock);
                        while (block.objectCount--) {
	                        var decoded = self.datumReader.read(self.decoder);
                            if (decoded !== null) self.push(decoded);
                        }
                        cb();
					}
                }
            });
        }
    },

    _readBlocks: function(cb) {
        var self = this;
        self._readBlock(function(err) {
            if (err || self._fileBlock.remainingBytes === 0) {
                cb();
            } else {
                self._readBlocks(cb);
            }
        });
    },

    _transform: function(chunk, encoding, done) {
        this._fileBlock.write(chunk);

        if (!this._inBody) {
            var header = this._readHeader();
            if (!(header instanceof AvroErrors.BlockDelayReadError))
                this._inBody = true;
        } 

        if (this._inBody) {
            this._readBlocks(done);
        } else
            done();
    },

    _flush: function(done) {

        if (this._fileBlock.remainingBytes > 0)
            this._readBlocks(done);
        else
            done();
    }

});

// Writer Class
function Writer(writersSchema, codec) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema, codec);

    Stream.call(this);
    this.readable = true;
    this.writable = true;
    this._streamOffset = 0;
    this._paused = false;
    this.codec = codec || "null";
    this._datumBlock = new Block();
    this._fileBlock = new Block();
    this._resetBlocks();
    this._writersSchema = writersSchema ? Avro.Schema(writersSchema) : null;
    this.datumWriter = IO.DatumWriter(this._writersSchema);
    this.encoder = IO.BinaryEncoder(this._fileBlock);
    this._inputSchema = writersSchema;
    return this;
}

util.inherits(Writer, Stream);

_.extend(Writer.prototype, {

    syncInterval: 1000 * SYNC_SIZE,

    _generateSyncMarker: function(size) {
        if (size < 1) return null;
        var marker = "";
        for (var i = 0; i < size; i++) {
            marker += String.fromCharCode(Math.floor(Math.random() * 0xFF));
        }
        return marker;
    },

    _metaData: function(codec, schema) {
        return {
            "avro.codec": codec ? codec : null,
            "avro.schema": _.isObject(schema) ? JSON.stringify(schema): schema
        };
    },

    _blockData: function(data) {
        return {
            "objectCount": this._blockCount,
            "objects": data,
            "sync": this.syncMarker
        }
    },

    _snappyCompress: function(data, callback) {
        var calculatedChecksum = crc32(data);
        snappy.compress(data, function(err, data) {
            if (err) return callback(err);
            // TODO: this might be a performance hit, having to create a new buffer just to add a crc32
            var checksumBuffer = new Buffer(data.length + 4);
            data.copy(checksumBuffer);
            checksumBuffer.writeUInt32BE(calculatedChecksum.readUInt32BE(0), checksumBuffer.length - 4);
            callback(null, checksumBuffer);
        });
    },

    compressData: function(data, codec, callback) {
        switch(codec) {
            case 'null':    callback(null, data);                   break;
            case 'deflate': zlib.deflateRaw(data, callback);        break;
            case 'snappy':  this._snappyCompress(data, callback);   break;
            default:
                callback(new AvroErrors.FileError("Unsupported codec %s", codec));
                break;
        }
    },

    _writeHeader: function(schema) {
        this.syncMarker = this._generateSyncMarker(SYNC_SIZE);
        var avroHeader = {
            'magic': magic(),
            'meta': this._metaData(this.codec, schema),
            'sync': this.syncMarker
        };
        this.datumWriter.writeData(metaSchema(), avroHeader, this.encoder);
        this.emit('data', this._datumBlock.toBuffer());
        this.encoder.output(this._datumBlock);
        return this._fileBlock.length;
    },

    _resetBlocks: function() {
        this._fileBlock.flush();
        this._datumBlock.flush();
        this._blockOffset = 0;
        this._blockCount = 0;
    },

    pause: function(){
        this._paused = true;
    },

    resume: function() {
        if (this.writable) {
            this._paused = false;
            this.emit('drain');
        }
    },

    _writeBlock: function(final) {
        var self = this;
        final = final || false;
        if (this._blockCount > 0) {
            this.compressData(this._datumBlock.toBuffer(), this.codec, function(err, buffer) {
                if (err) self.emit('error', err);
                self.encoder.output(self._fileBlock);
                self.datumWriter.writeData(blockSchema(), self._blockData(buffer), self.encoder);
                if (!self._paused) {
                    self.emit('data', self._fileBlock.toBuffer());
                    self._resetBlocks();
                    if (final) {
                        self.emit('end');
                        process.nextTick(function() {
                            self.destroy();
                        });
                    }
                }
                self.encoder.output(self._datumBlock);
            });
        } else {
        	if (final) {
        		self.emit('end');
                process.nextTick(function() {
                    self.destroy();
                });
        	}
        }
    },

    write: function(data) {
        if (_.isUndefined(data))
            throw new AvroErrors.FileError('no data passed to write()');

        if (this._streamOffset === 0)
            this._streamOffset += this._writeHeader(this._inputSchema);

        this.datumWriter.writeData(this._writersSchema, data, this.encoder);
        this._blockCount++;
        this._blockOffset += this._datumBlock.length;
        this._streamOffset += this._datumBlock.length;

        if (this._blockOffset > this.syncInterval) {
            this._writeBlock();
        }

        return !this._paused;
    },

    append: function(data) {
        this.write(data);
        return this;
    },

    end: function(data) {
        var self = this;
        if (this._paused) {
            process.nextTick(function() {
                self.end(data);
            });
        } else {
            var self = this;
            if (data) this.write(data);
            this._writeBlock(true);
            return self;
        }
    },

    destroy: function() {
        this._fileBlock = null;
        this._datumBlock = null;
        this.writable = false;
        this.readable = false;
        this.emit('close');
    }

});

if (!_.isUndefined(exports)) {
    exports.AvroFile = AvroFile;
    exports.Reader = Reader;
    exports.Writer = Writer;
    exports.Block = Block;
}
