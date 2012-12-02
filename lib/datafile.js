var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';

var fs = require('fs');
var util = require('util');
var zlib = require('zlib');
var snappy = require('snappy');
var crc32 = require('buffer-crc32');
var _ = require('underscore');
var Stream = require('stream').Stream;

IO = require(libpath + 'io');
Avro = require(libpath + 'schema');

// Constants
var VERSION = 1;
var SYNC_SIZE = 16;
var DEFAULT_BUFFER_SIZE = 8192;
var VALID_CODECS = ["null", "deflate", "snappy"];

// Error objects
var AvroFileError = function() { 
    return new Error('AvroFileError: ' + util.format.apply(null, arguments)); 
};   
    
var AvroBlockError = function() {
    return new Error('AvroBlockError: ' + util.format.apply(null, arguments));
};
    
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
                _operation = new Reader();
                var fileStream = fs.createReadStream(path, _options);
                fileStream.pipe(_operation);
                break; 
            case "w":
                var fileStream = fs.createWriteStream(path, _options);
                _operation = new Writer(schema, _options.codec);
                _operation.pipe(fileStream);
                break;
            default: 
                throw new AvroFileError("Unsupported operation %s on file", _options.flags);
        }
        return _operation;
    };
    
}

function Block(size) {
        
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(size);
            
    size = size || 0;
    this.length = 0;
    this.offset = 0;
    this._buffer = new Buffer(size);
    this.reUseBuffer = true;
}
    
Block.prototype = {
        
    flush: function() {
        this.length = 0;
    },
        
    _bufferSize: function(size) {
        if (!this._buffer.length) {
            return size;
        } else if (this._buffer.length < DEFAULT_BUFFER_SIZE) {
            var doubleSize = this._buffer.length * 2;
            return (doubleSize - this._buffer.length) > size ? doubleSize : this._buffer.length + size;
        } else {
            return this.length + size;
        }
    },
      
    _canReUseBuffer: function(size) {
        return this.reUseBuffer && this.offset >= size;
    },
      
    _resizeIfRequired: function(size) {
        if (this._canReUseBuffer(size)) {
            if (this.offset != this.length)
                this._buffer.copy(this._buffer, 0, this.offset, this.length);
            this.length = this.remainingBytes();
            this.offset = 0;
        } else if (this.length + size > this._buffer.length) {
            var oldBuffer = this._buffer;
            this._buffer = new Buffer(this._bufferSize(size));
            oldBuffer.copy(this._buffer, 0);
        }
    },
    
    remainingBytes: function() {
        return this.length - this.offset;
    },    
    
    skip: function(size) {
        if ((this.offset + size) > 0 && size <= this.remainingBytes())
            this.offset += size;
        else
            throw new AvroBlockError('tried to skip(%d) outsite of the block(%d) at %d', size, this.remainingBytes(), this.offset);
    },
    
    read: function(size) {
        var self = this;
        if (size > this.remainingBytes()) {
            process.nextTick(function() {
                self.read(size);
            });
//            throw new AvroBlockError('tried to read(%d) past the amount written to the block(%d) at %d', 
  //                                   size, this.remainingBytes(), this.offset);
        } else if (this.offset + size > this._buffer.length) {
            throw new AvroBlockError('tried to read(%d) outside of the buffer(%d) at %d', 
                                     size, this._buffer.length, this.offset);
        } else if (size < 0) {
            throw new AvroBlockError("Tried to read a negative amount of %d bytes", size);
        } else {
            this.offset += size;
            return this._buffer.slice(this.offset - size, this.offset);
        } 
    },
        
    write: function(value) {
        var len = (Buffer.isBuffer(value) || _.isArray(value)) ? value.length : 1;
        this._resizeIfRequired(len);
            
        if (Buffer.isBuffer(value)) {
            value.copy(this._buffer, this.length);
            this.length += value.length;
        } else if (_.isArray(value)) {
            var item;
            while (item = value.shift()) {
                this._buffer[this.length++] = item;
            }
        } else {
            this._buffer[this.length++] = value;
        }
    },
        
    isEqual: function(value) {
        if (Buffer.isBuffer(value) || _.isArray(value)) {
            for (var i = 0; i < value.length; i++) {
                if (this._buffer[i] !== value[i])
                    return false;
            }
        } else {
            throw new AvroBlockError("must supply an array or buffer")
        }
        return true;
    },

    slice: function(start, end) {
        start = start || 0;
        end = end || this.length;
        return this._buffer.slice(start, end);
    },

    toBuffer: function() {
        return this.slice();
    },
    
    toString: function(){
        return "Block: " + util.inspect(this.slice());
    }    
}

// Reader Class
function Reader(decoder) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    Stream.call(this);        
    this.writable = true;
    this._fileBlock = new Block();
    this._datumBlock = new Block();
    this._paused = false;
    this.decoder = decoder || IO.BinaryDecoder(this._fileBlock);
}

util.inherits(Reader, Stream);

_.extend(Reader.prototype, {
    
    write: function(newBuffer) {
        this._fileBlock.write(newBuffer);
        
        this.emit('data');
        return !this._paused;
    },
    
    pause: function() {
        this._paused = true;
    },
    
    resume: function() {
        if (this.writable) {
            this._paused = false;
            this.emit('drain');
        }
    },
    
    _snappyDecompress: function(rawData, callback) {
        var compressedData = rawData.slice(0, rawData.length - 4);
        var checksum = rawData.slice(rawData.length - 4, rawData.length);
        snappy.decompress(compressedData, function(err, data) {
            if (err) return callback(err);
            var calculatedChecksum = crc32(data);
            if (calculatedChecksum.readUInt32BE(0) !== checksum.readUInt32BE(0))
                callback(new AvroFileError("Failed checksum from decompressed snappy data block %d !== %d",
                    calculatedChecksum.readUInt32BE(0), checksum.readUInt32BE(0)));
            else 
                callback(null, data);
        }, snappy.parsers.raw);         
    },
    
    decompressData: function(data, codec, callback) {
        switch(codec) {
            case "null":    callback(null, data);                   break;
            case "deflate": zlib.inflateRaw(data, callback);        break;
            case "snappy":  this._snappyDecompress(data, callback); break;
            default:        
                callback(new AvroFileError("Unsupported codec %s", codec)); 
                break;
        }
    },
        
    readHeader: function(){
        var header = this.datumReader.readData(metaSchema(), null, this.decoder);
        if (!header || !header.magic || header.magic.toString() !== magic()) {
            throw new AvroFileError("Not an avro file %j", header);
        }
        this.codec = header.meta["avro.codec"].toString();
        this.syncMarker = header.sync;
        var schema = header.meta["avro.schema"].toString();
        try {
            schema = JSON.parse(schema);
        } catch(e) {}
        this.writersSchema = Avro.Schema(schema);
    },
        
    readBlock: function(callback) {
        var self = this;
        var offset = this._offset;

        var block = this.datumReader.readData(blockSchema(), null, this.decoder);
        if (block && block.objects) {
            // Check for a sync marker
           // if (block.sync.toString() !== this.syncMarker.toString()) {
            //    self._fileBlock.skip(-this.SYNC_SIZE);
        //    }

            this.decompressData(block.objects, this.codec, function(err, data) {
                if (err) callback(err);
                else {
                    self._datumBlock.write(data);
                    self.decoder.input(self._datumBlock);
                    for (var i = 0; i < block.objectCount; i++)
                        callback(null, self.datumReader.readData(self.writersSchema, null, self.decoder));    
                    self.decoder.input(self._fileBlock);
                }
            });
            return true;
        } else {
            // Go back to the previous read position
            this._offset = offset;
            return false;
        }
    },
        
    read: function(schema, callback) {
        if (!_.isFunction(callback))
            throw new AvroFileError("Must provide a callback function to read");
        
        var self = this;
        this.datumReader = IO.DatumReader(schema);
        var shouldReadHeader = true;
        this.on("data", function() {
            if (shouldReadHeader) {
                self.readHeader();
                shouldReadHeader = false;
            }
            // Need at least SYNC_SIZE bytes in the buffer to read a block
            while (self._fileBlock.remainingBytes() > SYNC_SIZE && self.readBlock(callback)) {}
        });
    },

    end: function(newBuffer) {
        var self = this;
        if (!_.isUndefined(newBuffer)) {
            this._fileBlock.write(newBuffer);
        }
        this.writable = false;
        this.emit('end');
        if (0 === this._fileBlock.remainingBytes()) {
            // Delay destroy till next run of event loop incase there is more data to process
            process.nextTick(function() {
                self._destroy();
            });
        }
        return true;
    },
    
    _destroy: function() {
        // TODO: fix this in the scenario when the blocks are null'd
        //this._fileBlock = null;
        //this._datumBlock = null;
        this.offset = 0;
        this.writable = false;
        this.emit('close');
    },
    
    close: function(callback) {
        callback();
    }
    
});
    
// Writer Class
function Writer(writersSchema, codec) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema, codec);
            
    this.readable = true;
    this.writable = true;
    this._paused = false;
    this.codec = codec;
    this._datumBlock = new Block();
    this._fileBlock = new Block();
    this._resetBlocks();
    this._writersSchema = writersSchema ? Avro.Schema(writersSchema) : null;
    this.datumWriter = IO.DatumWriter(this._writersSchema);
    this.encoder = IO.BinaryEncoder(this._datumBlock);
    this._writeHeader(writersSchema);
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
            "avro.codec": codec ? codec : "null",
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
            case "null":    callback(null, data);                   break;
            case "deflate": zlib.deflateRaw(data, callback);        break;
            case "snappy":  this._snappyCompress(data, callback);   break;
            default:
                callback(new AvroFileError("Unsupported codec %s", codec));
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
        this._resetBlocks();
    },

    _resetBlocks: function() {
        this._fileBlock.flush();
        this._datumBlock.flush();
        this._offset = 0;
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
    
    writeBlock: function(callback) {
        var self = this;
        if (this._blockCount > 0) {
            this.compressData(this._datumBlock.toBuffer(), this.codec, function(err, buffer) {
                if (err) return callback(err);
                self.encoder.output(self._fileBlock);
                self.datumWriter.writeData(blockSchema(), self._blockData(buffer), self.encoder);
                callback(self._fileBlock.toBuffer());
                self._resetBlocks();
                self.encoder.output(self._datumBlock);
            });
        } 
    },
        
    write: function(data) {
        var self = this;
        this.datumWriter.writeData(this._writersSchema, data, this.encoder);
        this._blockCount++;
        this._offset += this._datumBlock.length;
        
        console.log('writing');
        if (this._offset > this.syncInterval) {
            this.writeBlock(function(err, block) {
                if (err)
                    self.emit('error', err);
                else
                    self.emit('data', block);
            });
        } 
        
        return !this._paused;
    },
    
    end: function(data) {
        if (!_.isUndefined(data))
            this.write(data);
        this.writable = false;
        this.emit('end');
        if (0 === this._datumBlock.remainingBytes())
            this.destroy();
        else 
            process.nextTick(this.end(data));
    },
    
    destroy: function() {
        console.log('destroying');
        this._fileBlock = null;
        this._datumBlock = null;
        this.writable = false;
        this.readable = false;
        this.emit('close');
    }
    
    /*close: function(callback){
        var self = this;
        self.writeBlock(function(err) {
            //self.emit('close');
            //fs.closeSync(self._fd);
            if (!_.isUndefined(callback))
                callback();
        });
    }*/
        
});

if (!_.isUndefined(exports)) {
    exports.AvroFile = AvroFile;
    exports.Reader = Reader;
    exports.Writer = Writer;
    exports.Block = Block;
}