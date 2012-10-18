var fs = require("fs");
var zlib = require("zlib");
var _ = require("underscore");
var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var validator = require(__dirname + "/../other/validator").Validator;
var IO = require(libpath + "io");

var DataFile = function() {
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
};

require("util").inherits(DataFile, require("stream"));

DataFile.prototype = {
    
    VERSION: 1,
    SYNC_SIZE: 16,
    SYNC_INTERVAL: 1000 * this.SYNC_SIZE,
    VALID_CODECS: ["null", "deflate"],
    VALID_ENCODINGS: ["binary", "json"],            // Not used
    
    blockCount: 0,
    
    magic: function() {
        return "Obj" + String.fromCharCode(this.VERSION);
    },
    
    blockSchema: {
            "type": "record", "name": "org.apache.avro.Block",
            "fields" : [
               {"name": "objectCount", "type": "long" },
               {"name": "objects", "type": "bytes" },
               {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": this.SYNC_SIZE}}
            ]
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
            'meta': this.metaData(this.options.codec, this.schema),
            'sync': this.syncMarker
        };
        this.writer.writeData(this.metaSchema(), avroHeader, this.encoder);
        fs.writeSync(this.fd, this.encoder.buffer(), 0, this.encoder.idx);
        this.encoder.flush();
    },
    
    readHeader: function(){
        var schema = this.metaSchema();
        this.header = this.reader.readData(schema, schema, this.decoder);
        if (this.header.magic.toString() != this.magic()) {
            throw new Error("Not an avro file");
        }
        this.syncMarker = this.header.sync;
        this.readersSchema = this.header.meta["avro.schema"];
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
        }
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
                self.writer.writeData(self.blockSchema, self.blockData(buffer), self.encoder);
                fs.writeSync(self.fd, self.encoder.buffer(), 0, self.encoder.idx);
                self.encoder.flush();
                self.blockCount = 0;    
                callback(err);
            });
        }
    },
    
    readBlock: function(callback) {
        var self = this;
        var blockDecoder = IO.BinaryDecoder();
        var block = this.reader.readData(this.blockSchema, this.blockSchema, this.decoder);
        var codec = this.header.meta["avro.codec"].toString();
        this.decompressData(block.objects, codec, function(err, data) {
            if (err) 
                callback(err);
            else {
                blockDecoder.setBuffer(data);
                var schema = JSON.parse(self.header.meta["avro.schema"].toString());
                callback(null, self.reader.readData(schema, null, blockDecoder));    
            }
        });
    },
    
    write: function(data, callback) {
        this.writer.writeData(this.schema, data, this.encoder);
        this.blockCount++;
        
        if (this.encoder.idx > this.SYNC_INTERVAL) {
            this.writeBlock(this.encoder.buffer(), function(err) {
                callback(err);
            });
        } else
            callback();
    },
    
    open: function(path, schema, options) {
        this.schema = schema;        
        this.options = _.extend({ 
            codec:      "null", 
            flags:      'r',
            encoding:   null, 
            mode:       0666 
        }, options);

        if (this.VALID_CODECS.indexOf(this.options.codec) == -1)
            throw new Error("Unsupported codec " + this.options.codec);
            
        switch (this.options.flags) {
            case "r":
                this.decoder = IO.BinaryDecoder();
                this.reader = IO.DatumReader(schema);
                this.fd = fs.openSync(path, this.options.flags); 
                break; 
            case "w":
                this.encoder = IO.BinaryEncoder();
                this.writer = IO.DatumWriter(schema);
                this.fd = fs.openSync(path, this.options.flags); 
                this.writeHeader();
                break;
            default: 
                throw new Error("Unsupported operation on file " + this.options.flags);
                break;
        }
        return this;
    },
        
    read: function(callback) {
        var self = this;
        fs.read(this.fd, this.decoder.buf, 0, this.decoder.bufferSize, null, function(err) {
            self.readHeader();
            self.readBlock(function(err, data) {
                callback(err, data);                
            });
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