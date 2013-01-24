var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/';

var fs = require('fs'),
util = require('util'),
zlib = require('zlib'),
snappy = require('snappy'),
_ = require('underscore'),
Stream = require('stream').Stream,

IO = require(libpath + 'io');
Trevni = require(libpath + 'schema');

// Constants
var VERSION = 1;
var SYNC_SIZE = 16;
var DEFAULT_BUFFER_SIZE = 8192;
var VALID_CODECS = ["null", "deflate"]; //, "snappy"];

// Error objects
var TrevniFileError = function() { 
    return new Error('TrevniFileError: ' + util.format.apply(null, arguments)); 
};   
    
var TrevniBlockError = function() {
    return new Error('TrevniBlockError: ' + util.format.apply(null, arguments));
};
    
function magic() {
    return "Trv" + String.fromCharCode(VERSION);
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
                "name": "rowCount", 
                "type": {"type": "fixed", "name": "rowCount", "size": 8}
            },
            {
                "name": "columnCount", 
                "type": {"type": "fixed", "name": "columnCount", "size": 4}
            },
            {
                "name": "meta",
                "type": {
                    "type": "map",
                    "values": "bytes"
                }
            },
            {
                "name": "columnMetaData", 
                "type": {
                    "type": "array", 
                    "items": {
                        "type": "map",
                        "values": "bytes"
                    }
                }
            },
            {   "name": "columnOffsets", 
                "type": {
                    "type": "array", 
                    "items": {"type": "fixed", "name": "offset", "size": 8 }
                }
            }
        ]
    });
};
    
function blockSchema() {
    return Trevni.Schema({
        "type": "record", "name": "org.apache.avro.block",
        "fields" : [
            {"name": "objectCount", "type": "long" },
            {"name": "objects", "type": "bytes" },
            {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": SYNC_SIZE}}
        ]
    });
};
    
// TrevniFile Class
var TrevniFile = function() {
    
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
                _operation.decoder = IO.BinaryDecoder(_operation._fileBlock);
                var fileStream = fs.createReadStream(path, _options);
                fileStream.pipe(_operation);
                break; 
            case "w":
                _operation = new Writer();
                _operation.codec = _options.codec;
                _operation.encoder = IO.BinaryEncoder(_operation._datumBlock);
                _operation.writersSchema = Trevni.Schema(schema);
                _operation.datumWriter = IO.DatumWriter(_operation.writersSchema);
                _operation.fd = fs.openSync(path, _options.flags); 
                _operation.writeHeader(schema);
                break;
            default: 
                throw new TrevniFileError("Unsupported operation %s on file", _options.flags);
        }
        return _operation;
    };
    
    this.close = function(callback) {
        _operation.close(callback);
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
        if (size > 0 && size <= this.remainingBytes())
            this.offset += size;
        else
            throw new TrevniBlockError('tried to skip(%d) outsite of the block(%d) at %d', size, this.remainingBytes(), this.offset);
    },
    
    read: function(size) {
        if (size > this.remainingBytes()) {
            throw new TrevniBlockError('tried to read(%d) past the amount written to the block(%d) at %d', 
                                     size, this.remainingBytes(), this.offset);
        } else if (this.offset + size > this._buffer.length) {
            throw new TrevniBlockError('tried to read(%d) outside of the buffer(%d) at %d', 
                                     size, this._buffer.length, this.offset);
        } else if (size < 0) {
            throw new TrevniBlockError("Tried to read a negative amount of %d bytes", size);
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
            throw new TrevniBlockError("must supply an array or buffer")
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
function Reader() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    Stream.call(this);        
    this.writable = true;
    this._fileBlock = new Block();
    this._datumBlock = new Block();
    this._paused = false;
}

util.inherits(Reader, Stream);

_.extend(Reader.prototype, {
    
    write: function(newBuffer) {
        this._fileBlock.write(newBuffer);
        
        this.emit("data");
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
    
    decompressData: function(data, codec, callback) {
        switch(codec) {
            case "null":    callback(null, data);                                               break;
            case "deflate": zlib.inflateRaw(data, callback);                                    break;
            case "snappy":  snappy.decompress(data.slice(0,data.length-4), snappy.parsers.raw, callback);  break;
            default:        callback(new TrevniFileError("Unsupported codec %s", codec));         break;
        }
    },
        
    readHeader: function(){
        var header = this.datumReader.readData(metaSchema(), null, this.decoder);
        if (!header || !header.magic || header.magic.toString() !== magic()) {
            throw new TrevniFileError("Not an avro file %j", header);
        }
        this.codec = header.meta["avro.codec"].toString();
        this.syncMarker = header.sync;
        var schema = header.meta["avro.schema"].toString();
        try {
            schema = JSON.parse(schema);
        } catch(e) {}
        this.writersSchema = Trevni.Schema(schema);
    },
        
    readBlock: function(callback) {
        var self = this;
        var offset = this._offset;

        var block = this.datumReader.readData(blockSchema(), null, this.decoder);
        if (block && block.objects) {
            // Check for a sync marker
            if (block.sync.toString() !== this.syncMarker.toString()) {
                this.skip(-this.SYNC_SIZE);
            }

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
            throw new TrevniFileError("Must provide a callback function to read");
        
        if (_.isUndefined(this.decoder)) {
            this._fileBlock = new Block();
            this._datumBlock = new Block();
            this.decoder = IO.BinaryDecoder(this._fileBlock);
        }
        
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
function Writer() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
            
    this.readable = true;
    this._paused = false;
    this._datumBlock = new Block();
    this._fileBlock = new Block();
    this._resetBlocks();
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

    compressData: function(data, codec, callback) {        
        switch(codec) {
            case "null":    callback(null, data);                                        break;
            case "deflate": zlib.deflateRaw(data, callback);                             break;
            // TODO: add crc32 to end of snappy compressed block
            case "snappy":  snappy.compress(data, callback);                             break;
            default:        callback(new TrevniFileError("Unsupported codec %s", codec));  break;
        }
    },
        
    writeHeader: function(schema) {
        this.syncMarker = this._generateSyncMarker(SYNC_SIZE);
        var avroHeader = {
            'magic': magic(),
            'meta': this._metaData(this.codec, schema),
            'sync': this.syncMarker
        };
        this.datumWriter.writeData(metaSchema(), avroHeader, this.encoder);
        var fileBuffer = this._datumBlock.toBuffer();
        fs.writeSync(this.fd, fileBuffer, 0, fileBuffer.length);
        this._datumBlock.flush();
    },

    _resetBlocks: function() {
        this._fileBlock.flush();
        this._datumBlock.flush();
        this._offset = 0;
        this._blockCount = 0; 
    },
    
    writeBlock: function(callback) {
        var self = this;
        if (this._blockCount > 0) {
            this.compressData(this._datumBlock.toBuffer(), this.codec, function(err, buffer) {
                if (err) return callback(err);
                self.encoder.output(self._fileBlock);
                self.datumWriter.writeData(blockSchema(), self._blockData(buffer), self.encoder);
                var fileBuffer = self._fileBlock.toBuffer();
                fs.writeSync(self.fd, fileBuffer, 0, fileBuffer.length);
                self._resetBlocks();
                self.encoder.output(self._datumBlock);
                callback();
            });
        } else 
            callback();
    },
        
    write: function(data, callback) {
        this.datumWriter.writeData(this.writersSchema, data, this.encoder);
        this._blockCount++;
        this._offset += this._datumBlock.length;
        
        if (this._offset > this.syncInterval) {
            this.writeBlock(function(err) {
                callback(err);
            });
        } else
        callback();
    },
    
    close: function(callback){
        var self = this;
        self.writeBlock(function(err) {
            fs.closeSync(self.fd);
            if (!_.isUndefined(callback))
                callback();
        });
    }
        
});

if (!_.isUndefined(exports)) {
    exports.TrevniFile = TrevniFile;
    exports.Reader = Reader;
    exports.Writer = Writer;
    exports.Block = Block;
}