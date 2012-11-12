var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";

var fs = require("fs"),
util = require("util"),
zlib = require("zlib"),
snappy = require("snappy"),
_ = require("underscore"),
Stream = require("stream").Stream,
IO = require(libpath + "io");

// Constants
var VERSION = 1,
SYNC_SIZE = 16,
DEFAULT_BUFFER_SIZE = 8192,
VALID_CODECS = ["null", "deflate", "snappy"],
VALID_ENCODINGS = ["binary", "json"];            // Not used

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
    return {
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
                "values": "bytes"
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
    };
};
    
function blockSchema() {
    return {
        "type": "record", "name": "org.apache.avro.block",
        "fields" : [
        {"name": "objectCount", "type": "long" },
        {"name": "objects", "type": "bytes" },
        {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": SYNC_SIZE}}
        ]
    };
};
    
// AvroFile Class
var AvroFile = function() {
    
    if ((this instanceof arguments.callee) === false)
    return new arguments.callee();

    var _options = {}, 
    _blockCount = 0,
    _operation;
       
    // Public methods
    this.open = function(path, schema, options) {    
        _options = _.extend({ 
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
            _operation.writersSchema = schema;
            _operation.datumWriter = IO.DatumWriter(schema);
            _operation.fd = fs.openSync(path, _options.flags); 
            _operation.writeHeader();
            break;
            default: 
            throw new AvroFileError("Unsupported operation %s on file", _options.flags);
            break;
        }
        return _operation;
    };
    
    this.close = function(callback) {
        _operation.close(callback);
    };
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
        /*    var bytesAhead = this.bytesAhead();
        
        var reuseBuffer = (this._offset >= newBuffer.length);
        
        if (!this.writable) {
            var err = new Error('stream not writable');
            err.code = 'EPIPE';
            this.emit('error', err);
            return false;
        }
        
        if (reuseBuffer) {
            // move unread bytes forward to make room for the new
            this._buffer.copy(this._buffer, this._offset - newBuffer.length, this._offset);
            this._moveOffset(-newBuffer.length);

            // add the new bytes at the end
            newBuffer.copy(this._buffer, this._buffer.length - newBuffer.length);
            if (this._compact) {
                this.compact();
            }
        } else {
            var oldBuffer = this._buffer;

            // grow a new buffer that can hold both
            this._buffer = new Buffer(bytesAhead + newBuffer.length);

            // copy the old and new buffer into it
            oldBuffer.copy(this._buffer, 0, this._offset);
            newBuffer.copy(this._buffer, bytesAhead);
            this._setOffset(0);
        }*/
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
        if (VALID_CODECS.indexOf(codec) == -1)
            throw new Error("Unsupported codec " + codec);
            
        switch(codec) {
            case "null":    callback(null, data);                                    break;
            case "deflate": zlib.inflateRaw(data, callback);                         break;
            case "snappy":  snappy.decompress(data, snappy.parsers.raw, callback);   break;
            default:        callback(new Error("Unsupported codec " + codec));       break;
        }
    },
        
    readHeader: function(){
        var header = this.datumReader.readData(metaSchema(), null, this.decoder);
        if (!header || !header.magic || header.magic.toString() !== magic()) {
            throw new AvroFileError("Not an avro file %j", header);
        }
        this.codec = header.meta["avro.codec"].toString();
        this.syncMarker = header.sync;
        this.writersSchema = header.meta["avro.schema"].toString();
        try {
            this.writersSchema = JSON.parse(this.writersSchema);
        } catch(e) {}
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
            this._setOffset(offset);
            return false;
        }
    },
        
/*    readBytes: function(length) {
        if (length > this.bytesAhead()) {
            return null; 
        } else if (length == 1)
            return this._buffer[this._moveOffset(1)];
        else { 
            this._moveOffset(length);   
            var newBuffer = new Buffer(length);
            this._buffer.copy(newBuffer, 0, this._offset - length, this._offset);
            return newBuffer;
        }
    }, 
  */      
    read: function(schema, callback) {
        if (typeof callback !== 'function')
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
        this.on("end", function() {
            //    callback();
        })
    },
    /*
    skip: function(bytes) {
        if (bytes < 0) {
            this.emit('error', new AvroFileError('tried to skip outsite of the buffer(%d) at %d', this._buffer.length, this._offset));
        }
        this._moveOffset(bytes);
    },
    
    compact: function() {
        if (this._offset < 1) {
            return;
        }

        this._buffer = this._buffer.slice(this._offset);
        this._setOffset(0);
    },
    */
    end: function(newBuffer) {
        if (undefined !== newBuffer) {
            this._fileBlock.write(newBuffer);
        }
        this.writable = false;
        this.emit('end');
        if (0 === this._fileBlock.remainingBytes()) {
            this.destroy();
        }
        return true;
    },
    
    destroy: function() {
        this._buffer = null;
        this._offset = 0;
        this.writable = false;
        this.emit('close');
    },
    
    /*
    _setOffset: function(offset) {
        var self = this;
        if ((offset < 0) || (offset > this.bytesBuffered())) {
            //this.emit('error', new AvroFileError('tried to skip outsite of the buffer(%d) at %d, %d %d', this._buffer.length, this._offset, offset, this.bytesBuffered()));
            return this._offset;
        }
        this._offset = offset;

        // handle end()
        if (! this.writable && (this.bytesAhead() === 0)) {
            // delay since a read may be in progress
            process.nextTick(function () {
                self.destroy();
            });
        }
        return offset;
    },

    _moveOffset: function(relativeOffset) {
        var oldOffset = this._offset;
        this._setOffset(oldOffset + relativeOffset);
        return oldOffset;
    },
    */
    close: function(callback) {
        callback();
    }
    
});
    
function Block(size) {
        
    if ((this instanceof arguments.callee) === false)
    return new arguments.callee(size);
            
    size = size || 0;
    this.length = 0;
    this.offset = 0;
    this._buffer = new Buffer(size);
    this.reUseBuffer = false;
}
    
Block.prototype = {
        
    flush: function() {
        this.length = 0;
    },
        
    reset: function() {
        this.flush();
    },
        
    _bufferSize: function(size) {
        if (this._buffer.length == 0)
        return size;
        else if (this._buffer.length < DEFAULT_BUFFER_SIZE) {
            var doubleSize = this._buffer.length * 2;
            return (doubleSize - this._buffer.length) > size ? doubleSize : this._buffer.length + size;
        } else
        return this._buffer.length + DEFAULT_BUFFER_SIZE;
    },
        
    _resizeIfRequired: function(size) {
        if (this._canReUseBuffer(size)) {
            this._buffer.copy(this._buffer, this._offset - newBuffer.length, this._offset);
            this._moveOffset(-newBuffer.length);

            // add the new bytes at the end
            newBuffer.copy(this._buffer, this._buffer.length - newBuffer.length);
        } else if (this.length + size > this._buffer.length) {
            var oldBuffer = this._buffer;
            this._buffer = new Buffer(this._bufferSize(size));
            oldBuffer.copy(this._buffer, 0);
        }
    },
    
    _canReUseBuffer: function(size) {
        return this.reUseBuffer && this.offset >= size;
    },
    
    remainingBytes: function() {
        return this.length - this.offset;
    },    
    
    skip: function(size) {
        if (size > 0 && size <= this.remainingBytes())
            this.offset += size;
        else
            throw new AvroFileError('tried to skip outsite of the block(%d) at %d', this._buffer.length, this._offset);
    },
    
    read: function(size) {
        if (size > this.remainingBytes())
            return null;
        else if (size > 1) {
            this.offset += size;
            return this._buffer.slice(this.offset - size, this.offset);
        } else
            return this._buffer[this.offset++];
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
                
    toBuffer: function() {
        var newBuffer = new Buffer(this.length);
        this._buffer.copy(newBuffer, 0, 0, this.length);
        return newBuffer;
    },
    
    toString: function(){
        return "Block: " + util.inspect(this._buffer);
    }
}
    
// Writer Class
function Writer(options) {
    
    if ((this instanceof arguments.callee) === false)
    return new arguments.callee(options);
            
    this.readable = true;
    this._options = options || {};
    this._datumBlock = new Block();
    this._fileBlock = new Block();
    this._paused = false;
    this._offset = this._options.offset || 0;
    this._blockCount = 0;
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
            "avro.schema": typeof(schema) == 'object' ? JSON.stringify(schema): schema
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
        if (VALID_CODECS.indexOf(codec) == -1)
        throw new Error("Unsupported codec " + codec);
        
        switch(codec) {
            case "null":    callback(null, data);                               break;
            case "deflate": zlib.deflateRaw(data, callback);                    break;
            case "snappy":  snappy.compress(data, callback);                    break;
            default:        callback(new Error("Unsupported codec " + codec));  break;
        }
    },
        
    writeHeader: function() {
        this.syncMarker = this._generateSyncMarker(SYNC_SIZE);
        var avroHeader = {
            'magic': magic(),
            'meta': this._metaData(this.codec, this.writersSchema),
            'sync': this.syncMarker
        };
        this.datumWriter.writeData(metaSchema(), avroHeader, this.encoder);
        var fileBuffer = this._datumBlock.toBuffer();
        fs.writeSync(this.fd, fileBuffer, 0, fileBuffer.length);
        this._datumBlock.flush();
    },
        
    writeBlock: function(callback) {
        var self = this;
        if (this._blockCount > 0) {
            self.compressData(self._datumBlock.toBuffer(), self.codec, function(err, buffer) {
                if (err) return callback(err);
                self.encoder.output(self._fileBlock);
                self.datumWriter.writeData(blockSchema(), self._blockData(buffer), self.encoder);
                var fileBuffer = self._fileBlock.toBuffer();
                fs.writeSync(self.fd, fileBuffer, 0, fileBuffer.length);
                self._fileBlock.flush();
                self._datumBlock.flush();
                self._offset = 0;
                self._blockCount = 0;    
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
            callback();
        });
    }
        
});

if (typeof(module.exports) !== 'undefined') {
    module.exports.AvroFile = AvroFile;
    module.exports.Reader = Reader;
    module.exports.Writer = Writer;
    module.exports.Block = Block;
}