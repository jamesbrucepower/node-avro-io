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
                throw new AvroFileError("Unsupported operation %s on file", _options.flags);
        }
        return _operation;
    };
    
}

function Block(size, debug) {
        
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(size);
            
    size = size || 0;
    this._writeOffset = 0;
    this._readOffset = 0;
    this._buffer = new Buffer(size);
    this.reUseBuffer = true;
	this.debug = debug || false;
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
        }
    }, 
    
    skip: function(size) {
        if ((this._readOffset + size) > 0 && size <= this.remainingBytes)
            this._readOffset += size;
        else
            throw new AvroBlockError('tried to skip(%d) outsite of the block(%d) at %d', size, this.remainingBytes, this._readOffset);
    },
    
    read: function(size) {
        var self = this;
        if (size > this.remainingBytes) {
            return null;
//            throw new AvroBlockError('tried to read(%d) past the amount written to the block(%d) at %d', 
  //                                   size, this.remainingBytes, this._readOffset);
        } else if (this._readOffset + size > this._buffer.length) {
            throw new AvroBlockError('tried to read(%d) outside of the buffer(%d) at %d', 
                                     size, this._buffer.length, this._readOffset);
        } else if (size < 0) {
            throw new AvroBlockError("Tried to read a negative amount of %d bytes", size);
        } else {
            this._readOffset += size;
            return this._buffer.slice(this._readOffset - size, this._readOffset);
        } 
    },
        
    write: function(value) {
		if (this.debug)
			console.log("%d: %s", this._writeOffset, value.toString());
        var len = (Buffer.isBuffer(value) || _.isArray(value)) ? value.length : 1;
        this._resizeIfRequired(len);
            
        if (Buffer.isBuffer(value)) {
            value.copy(this._buffer, this._writeOffset);
            this._writeOffset += value.length;
        } else if (_.isArray(value)) {
            var item;
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
            throw new AvroBlockError("must supply an array or buffer")
        }
        return true;
    },

    slice: function(start, end) {
        start = start || 0;
        end = end || this._writeOffset;
        return this._buffer.slice(start, end);
    },

    toBuffer: function() {
        return this.slice();
    },
    
    toString: function() {
        return "Block: " + util.inspect(this.slice());
    }    
});

// Reader Class
function Reader(schema, decoder) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(schema, decoder);
        
    Stream.call(this);        
    this.writable = true;
    this._fileBlock = new Block();
    this._datumBlock = new Block();
    this._streamOffset = 0;
    this._paused = false;
    this.decoder = decoder || IO.BinaryDecoder(this._fileBlock);
    this.datumReader = IO.DatumReader(null, schema);
}

util.inherits(Reader, Stream);

_.extend(Reader.prototype, {
    
    pause: function() {
        if (!this._paused) {
           this._paused = true;
           this.emit('pause');
        }
    },
    
    resume: function() {
        if (this.writable && this._paused) {
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
        
    _readHeader: function() {
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
        this.datumReader.writersSchema = this.writersSchema;
        return this._fileBlock.offset;
    },
        
    _readBlock: function(final) {
        var self = this;
        final = final || false;

        var block = this.datumReader.readData(blockSchema(), null, this.decoder);
        if (block && block.objects) {
            // Check for a sync marker
            if (block.sync && block.sync.toString() !== this.syncMarker.toString()) {
                //console.error("going back %d", SYNC_SIZE);
                self._fileBlock.skip(-SYNC_SIZE);
            }

            this.decompressData(block.objects, this.codec, function(err, data) {
                if (err) self.emit('error', err);
                else {
                    self._datumBlock.write(data);
                    self.decoder.input(self._datumBlock);
                    for (var i = 0; i < block.objectCount; i++)
                        self.emit('data', self.datumReader.read(self.decoder));
                    if (final) self.emit('end');
                    self.decoder.input(self._fileBlock);
                }
            });
            return true;
        } else {
            console.error("delaying... and trying again");
            process.nextTick(function() {
                self._readBlock(final);
            });
        }
    },
        
    write: function(newBuffer) {
        if (this.listeners('data').length == 0)
            throw new AvroFileError('No listeners setup for this instance');

        this._fileBlock.write(newBuffer);
            
        if (this._streamOffset === 0) {
            this._streamOffset += this._readHeader();
        } 
        
        while (this._fileBlock.remainingBytes > SYNC_SIZE) {
            this._readBlock();
        }
        
        return !this._paused;
    },

    end: function(newBuffer) {
        var self = this;
        if (!_.isUndefined(newBuffer)) {
            this._fileBlock.write(newBuffer);
        }
        this._readBlock(true);
        this.writable = false;
        this.emit('end');
        if (0 === this._fileBlock.remainingBytes) {
            // Delay destroy till next run of event loop incase there is more data to process
            process.nextTick(function() {
                self.destroy();
            });
        }
        return true;
    },
    
    destroy: function() {
        this._fileBlock = null;
        this._datumBlock = null;
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
            
    Stream.call(this);    
    this.readable = true;
    this.writable = true;
    this._streamOffset = 0;
    this._paused = false;
    this.codec = codec || "null";
    this._datumBlock = new Block(0, false);
    this._fileBlock = new Block(0, false);
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
        //this.emit('data', this._datumBlock.toBuffer());
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
		console.log("calling pause()");
        this._paused = true;
    },
    
    resume: function() {
		console.log("calling resume()");
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
                    console.log('emitting data()');
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
        } 
    },
        
    write: function(data) {
        if (_.isUndefined(data)) 
			throw new AvroFileError('no data passed to write()');
        
        if (this._streamOffset === 0)
            this._streamOffset += this._writeHeader(this._inputSchema);
            
        this.datumWriter.writeData(this._writersSchema, data, this.encoder);
        this._blockCount++;
        this._blockOffset += this._datumBlock.length;
        this._streamOffset += this._datumBlock.length;
        
        if (this._blockOffset > this.syncInterval) {
            console.log("writing block at %d", this._blockOffset);
            this._writeBlock();
        }
		        
        return !this._paused;
    },
    
    append: function(data) {
		return (this.write(data) ? this : null);
    },
    
    end: function(data) {
        var self = this;
        if (this._paused)
            process.nextTick(function() {
                self.end(data);
            });
        else {                
            console.log('calling end()');
            var self = this;
    		if (data) this.write(data);
            this.writable = false;
            this._writeBlock(true);	
            return self;
        }
    },
    
    destroy: function() {
		console.log("calling destroy()");
		if (this._datumBlock.remainingBytes > 0) 
			console.error("still have %d left", this._datumBlock.remainingBytes);
		else {
	        this._fileBlock = null;
	        this._datumBlock = null;
	        this.writable = false;
	        this.readable = false;
	        this.emit('close');
		}
    }
        
});

if (!_.isUndefined(exports)) {
    exports.AvroFile = AvroFile;
    exports.Reader = Reader;
    exports.Writer = Writer;
    exports.Block = Block;
}