var fs = require("fs");
var zlib = require("zlib");
var util = require("util");
var Stream = require("stream").Stream;
var snappy = require("snappy");
var _ = require("underscore");

var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var validator = require(__dirname + "/../other/validator").Validator;
var IO = require(libpath + "io");

var AvroFileError = function() { 
    return new Error('AvroFileError: ' + util.format.apply(null, arguments)); 
};

function Reader(options) {
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

module.exports = Reader;
util.inherits(Reader, Stream);

Reader.prototype = {
    
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
    
    read: function(length) {
        if (this._offset >= this._buffer.length)
            throw new Error("oob for readByte in current Buffer");
        else if (length == 1)
            return this.buf[this._moveOffset(1)];
        else { 
            var oldOffset = this._moveOffset(length);   
            var newBuffer = new Buffer(length);
            this._buffer.copy(newBuffer, 0, oldOffset, this._offset);
            return newBuffer;
        }
    }, 
    
    skip: function(bytes) {        
        if (bytes < 0) {
            this.emit('error', new Error('tried to skip outsite of the buffer'));
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
            this.emit('error', new Error('tried to read outsite of buffer boundary'));
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
}

var DataFile = function() {
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
};

DataFile.prototype = {
    
    VERSION: 1,
    SYNC_SIZE: 16,
    SYNC_INTERVAL: 1000 * this.SYNC_SIZE,
    VALID_CODECS: ["null", "deflate", "snappy"],
    VALID_ENCODINGS: ["binary", "json"],            // Not used
    
    blockCount: 0,
    
    magic: function() {
        return "Obj" + String.fromCharCode(this.VERSION);
    },
    
    blockSchema: function() {
        return {
            "type": "record", "name": "org.apache.avro.Block",
            "fields" : [
               {"name": "objectCount", "type": "long" },
               {"name": "objects", "type": "bytes" },
               {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": this.SYNC_SIZE}}
            ]
        };
    },
    
    blockData: function(datum) {
        return {
            "objectCount": this.blockCount,
            "objects": datum,
            "sync": this.syncMarker 
        };
    },
    
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
    
    metaSchema: function() {
        return {
            "type": "record", 
            "name": "org.apache.avro.file.Header",
            "fields" : [
                { 
                    "name": "magic", 
                    "type": {
                        "type": "fixed", 
                        "name": "magic", 
                        "size": this.magic().length
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
                        "size": this.SYNC_SIZE
                    }
                }
            ]
        };
    },
     
    writeHeader: function() {
        this.syncMarker = this.generateSyncMarker(this.SYNC_SIZE);
        var avroHeader = {
            'magic': this.magic(),
            'meta': this.metaData(this.options.codec, this.writersSchema),
            'sync': this.syncMarker
        };
        this.writer.writeData(this.metaSchema(), avroHeader, this.encoder);
        fs.writeSync(this.fd, this.encoder.buffer(), 0, this.encoder.idx);
        this.encoder.flush();
    },
    
    readHeader: function(){
        var schema = this.metaSchema();
        var header = this.datumReader.readData(schema, null, this.decoder);
        if (header.magic.toString() != this.magic()) {
            throw new AvroFileError("Not an avro file");
        }
        this.options.codec = header.meta["avro.codec"].toString();
        this.syncMarker = header.sync;
        this.writersSchema = header.meta["avro.schema"].toString();
        try {
            this.writersSchema = JSON.parse(this.writersSchema);
        } catch(e) {}
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

    decompressData: function(data, codec, callback) {
        switch(codec) {
            case "null": 
                callback(null, data); 
                break;
            case "deflate":
                //console.log("decompressing %d", data.length);
                zlib.inflateRaw(data, function(err, buffer) {
                    //if (!err)
                     //   console.log("uncompressed %d", buffer.length);
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

    
    writeBlock: function(data, callback) {
        var self = this;
        if (this.blockCount > 0) {
            self.compressData(data, this.options.codec, function(err, buffer) {
                self.encoder.flush();
                self.writer.writeData(self.blockSchema(), self.blockData(buffer), self.encoder);
                fs.writeSync(self.fd, self.encoder.buffer(), 0, self.encoder.idx);
                self.encoder.flush();
                self.blockCount = 0;    
                callback(err);
            });
        }
    },
    
    readBlock: function(callback) {
        var self = this;
        var blockDecoder = IO.BinaryDecoder()
        console.log("____ about to read at %d", this._offset);
        var block = this.datumReader.readData(this.blockSchema(), null, this.decoder);
        if (block.sync.toString() !== this.syncMarker.toString()) {
            this.decoder.seek(-this.SYNC_SIZE);
        } else 
            this._offset += this.decoder.idx;
        console.log("blockCount %d, blockSize %d, blockSync %s, Buffer Offset %d", 
                    block.objectCount, block.objects.length, block.sync.inspect(), this._offset);
        this.decompressData(block.objects, self.options.codec, function(err, data) {
            if (err) 
                callback(err);
            else {
                blockDecoder.setBuffer(data);
                for (var i = 0; i < block.objectCount; i++) {
                    callback(null, self.reader.readData(self.writersSchema, null, blockDecoder));    
                }
            }
        });
        return this.decoder.idx;
    },
    
    write: function(data, callback) {
        this.writer.writeData(this.writersSchema, data, this.encoder);
        this.blockCount++;
        
        if (this.encoder.idx > this.SYNC_INTERVAL) {
            this.writeBlock(this.encoder.buffer(), function(err) {
                callback(err);
            });
        } else
            callback();
    },
    
    open: function(path, schema, options) {    
        this.options = _.extend({ 
            codec:      "null", 
            flags:      'r',
            encoding:   null, 
            mode:       0666, 
            bufferSize: 64 * 1024
        }, options);

        if (this.VALID_CODECS.indexOf(this.options.codec) == -1)
            throw new Error("Unsupported codec " + this.options.codec);
            
        switch (this.options.flags) {
            case "r":
                this.reader = new Reader();
                this.decoder = IO.BinaryDecoder(this.reader);
                this.readersSchema = schema;
                this.datumReader = IO.DatumReader(schema);
                this.stream = fs.createReadStream(path, this.options);
                break; 
            case "w":
                this.encoder = IO.BinaryEncoder();
                this.writersSchema = schema;
                this.writer = IO.DatumWriter(schema);
                this.fd = fs.openSync(path, this.options.flags); 
                this.writeHeader();
                break;
            default: 
                throw new AvroFileError("Unsupported operation %s on file", this.options.flags);
                break;
        }
    },
        
    read: function(callback) {
        var self = this;
        var shouldReadHeader = true;
        this.stream.pipe(this.reader);

        this.reader.on("data", function() {
            console.log("offset %d", this.reader._offset);
            if (shouldReadHeader) {
                self.readHeader();
                //console.log("read header of %d bytes, buffer is %d", self._offset, buffer.length);
                shouldReadHeader = false;
            }
            while (self.reader.bytesAhead()) {
                console.log("reader offset %d", self.reader._offset);
                self.readBlock(callback);   
            }
        });
        this.stream.on("end", function() {
            //callback();
        });
    },
    
    close: function() {
        var self = this;
        if (this.blockCount > 0)
            self.writeBlock(self.encoder.buffer(), function(err) {
                fs.closeSync(self.fd);
            });
    },
    
    end: function() {
        this.close();
    }
}

if (typeof(module.exports) !== 'undefined') {
    module.exports = DataFile;
}