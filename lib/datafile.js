var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/';

var fs = require('fs');
var util = require('util');
var zlib = require('zlib');
var snappy = require('snappy');
var crc32 = require('buffer-crc32');
var _ = require('underscore');
var Stream = require('stream').Stream;

var IO = require(libpath + 'io');
var Avro = require(libpath + 'schema');
var AvroErrors = require(libpath + 'errors');

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

function Block(size, debug) {
        
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(size);
            
    size = size || 0;
    this._writeOffset = 0;
    this._readOffset = 0;
    this._buffer = new Buffer(size);
    this.reUseBuffer = true;
    this.totalRead = 0;
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
        } else if (size < 0) {
            throw new AvroErrors.BlockError("Tried to read a negative amount of %d bytes", size);
        } else {
            this.totalRead += size;
            //if (this.debug && size > 2000)
             //   console.error('reading %d, total read %d', size, this.totalRead);
                
            this._readOffset += size;
            return this._buffer.slice(this._readOffset - size, this._readOffset);
        } 
    },
        
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
    this._fileBlock = new Block(0, true);
    this._datumBlock = new Block();
    this._streamOffset = 0;
    this._paused = false;
    this.decoder = decoder || IO.BinaryDecoder(this._fileBlock);
    this.datumReader = IO.DatumReader(null, schema);
    this.blockNum = 0;
}

util.inherits(Reader, Stream);

_.extend(Reader.prototype, {
    
    pause: function() {
        console.error("calling Reader pause()");
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
                callback(new AvroErrors.FileError("Failed checksum from decompressed snappy data block %d !== %d",
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
                throw header;
        } else if (header.magic.toString() !== magic()) {
            throw new AvroErrors.FileError("Not an avro file %j", header);
        }
        this.codec = header.meta["avro.codec"].toString();
        this.syncMarker = header.sync;
        var schema = header.meta["avro.schema"].toString();
        try {
            schema = JSON.parse(schema);
        } catch(e) {}
        //console.error(header);
        //console.error(JSON.stringify(schema, 0, 4));
        this.writersSchema = Avro.Schema(schema);
        this.datumReader.writersSchema = this.writersSchema;
        return this._fileBlock.offset;
    },
        
    _readBlock: function(final) {
        var self = this;
        final = final || false;

        this.blockNum++;
        var oldOffset = this._fileBlock.offset;
        var block = this.datumReader.readData(blockSchema(), null, this.decoder);
        this._streamOffset += (this._fileBlock.offset  - oldOffset);
        if (block instanceof AvroErrors.BlockDelayReadError) {
            //console.error("delaying... and trying again for blk %d, remaining bytes %d", this.blockNum, this._fileBlock.remainingBytes);
            if (!final)
                process.nextTick(function() {
                    self._readBlock(final);
                });
            return false;
        } else {
            // Check for a sync marker
            //console.error("Offset %d, remaining %d, block(%d) %s", this._fileBlock.offset, this._fileBlock.remainingBytes, block.objects.length, util.inspect(block));
            if (block.sync && block.sync.toString() !== this.syncMarker.toString()) {
                console.error("going back %d " + util.inspect(block) + " (%d)", SYNC_SIZE, block.objects ? block.objects.length : -1);
                self._fileBlock.skip(-SYNC_SIZE);
                /*process.nextTick(function() {
                    self._readBlock(final);
                });*/
                return false;
            }

            //console.error("decompressing %d bytes", block.objects.length);
            block.blockNum = this.blockNum;
            this.decompressData(block.objects, this.codec, function(err, data) {
                if (err) {} //self.emit('error', util.format("blockNum %d: %j", block.blockNum, err));
                else {
                    //console.error("%d decompressed %d blocks %d -> %d bytes, size per record comp:%d, uncomp:%d", block.blockNum, block.objectCount, block.objects.length, data.length, Math.round(data.length / block.objectCount), Math.round(block.objects.length / block.objectCount));
                    self._datumBlock.write(data);
                    self.decoder.input(self._datumBlock);
                    for (var i = 0; i < block.objectCount; i++)
                        self.emit('data', self.datumReader.read(self.decoder));
                    if (final) {
                        self.emit('end');
                        self._datumBlock = null;
                    }
                    self.decoder.input(self._fileBlock);
                }
            });
            return true;  
        } 
    },
        
    write: function(newBuffer) {
        console.error('adding %d bytes to %d/%d', newBuffer.length, this._fileBlock.offset, this._fileBlock.length);
        //console.error('%d STREAM %d, FILEBLOCK %d/%d, DATUMBLOCK %d/%d', this.blockNum, this._streamOffset, this._fileBlock.offset, this._fileBlock.length, this._datumBlock.offset, this._datumBlock.length);
        if (this.listeners('data').length == 0)
            throw new AvroErrors.FileError('No listeners setup for this instance');

        this._fileBlock.totalRead = 0;
        this._fileBlock.write(newBuffer);

            
        if (this._streamOffset === 0) {
            var header = this._readHeader();
            if (!(header instanceof AvroErrors.BlockDelayReadError)) 
                this._streamOffset += header;
        } 
        
        if (this._streamOffset > 0)
            while (this._fileBlock.remainingBytes > SYNC_SIZE && this._readBlock()) {
                //console.error('_____REMAIN________ %d', this._fileBlock.remainingBytes);
            }
        
        return !this._paused;
    },

    end: function(newBuffer) {
        var self = this;
        if (!_.isUndefined(newBuffer)) {
            this._fileBlock.write(newBuffer);
        }
        var duplicated = false;
        var previousOffset = 0;

        while (this._fileBlock.remainingBytes > SYNC_SIZE && this._fileBlock.offset !== previousOffset) {
            //console.error('FIN_____REMAIN________ %d/%d', this._fileBlock.offset, this._fileBlock.remainingBytes);
            console.error('end %d STREAM %d, FILEBLOCK %d/%d, DATUMBLOCK %d/%d ***', this.blockNum, this._streamOffset, this._fileBlock.offset, this._fileBlock.length, this._datumBlock.offset, this._datumBlock.length);
            this.blockNum++;
            this._readBlock();
            previousOffset = this._fileBlock.offset;
        }
        this._readBlock(true);
        this.writable = false;
        this.emit('end');
        if (0 === this._fileBlock.remainingBytes) {
            // Delay destroy till next run of event loop incase there is more data to process
            process.nextTick(function() {
                self.destroy();
            });
        } else
            console.error('GOING TO LEAK!!!');
        return true;
    },
    
    destroy: function() {
        this._fileBlock = null;
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
        console.error("calling Writer pause()");
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
            throw new AvroErrors.FileError('no data passed to write()');
        
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