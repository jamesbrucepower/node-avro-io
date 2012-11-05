var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";

var fs = require("fs"),
    util = require("util"),
    zlib = require("zlib"),
    snappy = require("snappy"),
    _ = require("underscore"),
    Stream = require("stream").Stream,
    IO = require(libpath + "io");

var DataFile = function() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();

    // Error object
    var AvroFileError = function() { 
        return new Error('AvroFileError: ' + util.format.apply(null, arguments)); 
    };        
    
    // Constants
    var VERSION = 1,
        SYNC_SIZE = 16,
        DEFAULT_BUFFER_SIZE = 8192,
        VALID_CODECS = ["null", "deflate", "snappy"],
        VALID_ENCODINGS = ["binary", "json"];            // Not used
    
    var _options = {}, 
        _blockCount = 0;
    
    // Private methods
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
    
    function blockData(datum) {
        return {
            "objectCount": _blockCount,
            "objects": datum,
            "sync": this.syncMarker 
        };
    };
    
    this.setCodec = function(codec) {
        if (VALID_CODECS.indexOf(codec) == -1)
            throw new Error("Unsupported codec " + _options.codec);
            
        _options.codec = codec;
    };
    
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
                this.Reader.decoder = IO.BinaryDecoder(this.Reader);
                var fileStream = fs.createReadStream(path, _options);
                fileStream.pipe(this.Reader);
                return this.Reader;
                break; 
            case "w":
                this.setCodec(_options.codec);
                this.Writer.encoder = IO.BinaryEncoder(this.Writer);
                this.Writer.writersSchema = schema;
                this.Writer.writer = IO.DatumWriter(schema);
                this.Writer.fd = fs.openSync(path, _options.flags); 
                this.Writer.writeHeader();
                return this.Writer;
                break;
            default: 
                throw new AvroFileError("Unsupported operation %s on file", _options.flags);
                break;
        }
    };
    
    this.close = function() {
        var self = this;
        if (this.Writer.encoder && _blockCount > 0)
            self.Writer.writeBlock(self.Writer._buffer, function(err) {
                fs.closeSync(self.Writer.fd);
            });
    };
    
    // Reader Class
    function Reader(options) {
    
        if ((this instanceof arguments.callee) === false)
            return new arguments.callee(options);
        
        Stream.call(this);
    
        if (Buffer.isBuffer(options)) 
            options = { buffer: options };
        
        options = options || {};
        this.writable = true;
        this._buffer = options.buffer || new Buffer(0);
        this._compact = options.compact || false;
        this._paused = false;
        this._offset = options.offset || 0;
    }

    util.inherits(Reader, Stream);

    _.extend(Reader.prototype, {
    
        write: function(newBuffer) {
            var bytesAhead = this.bytesAhead();
        
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
            }
        
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
    
        bytesAhead: function() {
            return this._buffer.length - this._offset;
        },
    
        bytesBuffered: function() {
            return this._buffer.length;
        },
    
        decompressData: function(data, codec, callback) {
            switch(codec) {
                case "null": 
                    callback(null, data); 
                    break;
                case "deflate":
                    zlib.inflateRaw(data, function(err, buffer) {
                        callback(err, buffer);
                    });                
                    break;
                case "snappy":
                    snappy.decompress(data, snappy.parsers.raw, function(err, buffer) {
                        callback(err, buffer);
                    });
                    break;
                default: 
                    callback(new Error("Unsupported codec " + codec));
                    break;
            }
        },
        
        readHeader: function(){
            var header = this.datumReader.readData(metaSchema(), null, this.decoder);
            if (!header || !header.magic || header.magic.toString() != magic()) {
                throw new AvroFileError("Not an avro file %j", header);
            }
            _options.codec = header.meta["avro.codec"].toString();
            this.syncMarker = header.sync;
            this.writersSchema = header.meta["avro.schema"].toString();
            try {
                this.writersSchema = JSON.parse(this.writersSchema);
            } catch(e) {}
        },
        
        readBlock: function(callback) {
            var self = this;
            var blockReader = Reader();
            var blockDecoder = IO.BinaryDecoder(blockReader)
            var offset = this._offset;

            var block = this.datumReader.readData(blockSchema(), null, this.decoder);
            if (block && block.objects) {
                // Check for a sync marker
                if (block.sync.toString() !== this.syncMarker.toString()) {
                    this.skip(-this.SYNC_SIZE);
                }
                this.decompressData(block.objects, _options.codec, function(err, data) {
                    if (err) callback(err);
                    else {
                        blockReader.write(data);
                        for (var i = 0; i < block.objectCount; i++)
                            callback(null, self.datumReader.readData(self.writersSchema, null, blockDecoder));    
                    }
                });
                return true;
            } else {
                // Go back to the previous read position
                this._setOffset(offset);
                return false;
            }
        },
        
        readBytes: function(length) {
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
        
        read: function(schema, callback) {
            var self = this;
            this.datumReader = IO.DatumReader(schema);
            var shouldReadHeader = true;
            this.on("data", function() {
                if (shouldReadHeader) {
                    self.readHeader();
                    shouldReadHeader = false;
                }
                // Need at least SYNC_SIZE bytes in the buffer to read a block
                while (self.bytesAhead() > SYNC_SIZE && self.readBlock(callback)) {}
            });
        },
    
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
    
        end: function(newBuffer) {
            if (undefined !== newBuffer) {
                this.write(newBuffer);
            }
            this.writable = false;
            this.emit('end');
            if (0 === this.bytesAhead()) {
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
        }
    });
    
    function Writer(options) {
    
        options = options || {};
        
        this.readable = true;
        this._buffer = options.buffer || new Buffer(DEFAULT_BUFFER_SIZE);
        this._paused = false;
        this._offset = options.offset || 0;
    }
    
    util.inherits(Writer, Stream);
    
    _.extend(Writer.prototype, {
        
        syncInterval: 1000 * SYNC_SIZE,
        
        generateSyncMarker: function(size) {
            var marker = "";
            for (var i = 0; i < size; i++) {
                marker += String.fromCharCode(Math.floor(Math.random() * 0xFF));
            }
            return marker;
        },
                
        metaData: function(codec, schema) {
            return {
                "avro.codec": codec ? codec : "null",
                "avro.schema": typeof(schema) == 'object' ? JSON.stringify(schema): schema
            };
        },
        
        compressData: function(data, codec, callback) {
            switch(codec) {
                case "null": 
                    callback(null, data); 
                    break;
                case "deflate":
                    zlib.deflateRaw(data, function(err, buffer) {
                        callback(err, buffer);
                    });                
                    break;
                case "snappy":
                    snappy.compress(data, function(err, buffer) {
                        callback(err, buffer);
                    });
                    break;
                default:
                    callback(new Error("Unsupported codec " + codec));
                    break;    
            }
        },
        
        writeHeader: function() {
            this.syncMarker = this.generateSyncMarker(this.SYNC_SIZE);
            var avroHeader = {
                'magic': magic(),
                'meta': this.metaData(_options.codec, this.writersSchema),
                'sync': this.syncMarker
            };
            this.writer.writeData(metaSchema(), avroHeader, this.encoder);
            fs.writeSync(this.fd, this._buffer, 0, this._offset);
            this._offset = 0;
        },
        
        writeBlock: function(data, callback) {
            var self = this;
            if (_blockCount > 0) {
                self.compressData(data, _options.codec, function(err, buffer) {
                    //self.encoder.flush();
                    self.writer.writeData(blockSchema(), blockData(buffer), self.encoder);
                    fs.writeSync(self.fd, self._buffer, 0, self._offset);
                    self._offset = 0;
                    _blockCount = 0;    
                    callback(err);
                });
            }
        },
        
        write: function(data, callback) {
            this.writer.writeData(this.writersSchema, data, this.encoder);
            _blockCount++;
        
            if (this._offset > this.SYNC_INTERVAL) {
                this.writeBlock(this._buffer, function(err) {
                    callback(err);
                });
            } else
                callback();
        },
        
        writeBytes: function(value) {
            if (Buffer.isBuffer(value)) {
                value.copy(this._buffer, this._offset);
                this._offset += value.length;
            } else {
                this._buffer[this._offset++] = value;
            }
        },
        
        value: function() {
            var newBuffer = new Buffer(this._buffer.length);
            this._buffer.copy(newBuffer, 0, this._offset);
            return newBuffer;
        }
        
    });
    
    this.Reader = new Reader();
    this.Writer = new Writer();
}

if (typeof(module.exports) !== 'undefined') {
    module.exports = DataFile;
}