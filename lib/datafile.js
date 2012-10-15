var fs = require("fs");
var zlib = require("zlib");
var _ = require("underscore");
var validator = require(__dirname + "/../lib/validator").Validator;
var IO = require("./io");

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
            "avro.schema": JSON.stringify(schema)
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
    
    compressData: function(data, callback) {
        switch(this.options.codec) {
            case "null": callback(null, data); break;
            case "deflate": {
                zlib.deflateRaw(data, function(err, buffer) {
                    callback(err, buffer);
                });                
                break;
            }
        }
    },
    
    writeBlock: function(data, callback) {
        if (this.blockCount > 0) {
            (function(self) {
                self.compressData(data, function(err, buffer) {
                    self.encoder.flush();
                    self.writer.writeData(self.blockSchema, self.blockData(buffer), self.encoder);
                    fs.writeSync(self.fd, self.encoder.buffer(), 0, self.encoder.idx);
                    self.encoder.flush();
                    self.blockCount = 0;    
                    callback(err);
                });
            })(this);
        }
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
        this.options = _.extend({ codec: "null", flags: 'r', encoding: null, mode: 0666 }, options);

        if (this.VALID_CODECS.indexOf(this.options.codec) == -1)
            throw new Error("Unsupported codec " + this.options.codec);
            
        switch (this.options.flags) {
            case "r": {
                this.reader = IO.DatumReader();
                this.fd = fs.openSync(path, this.options.flags); 
                break; 
            }
            case "w": {
                this.encoder = IO.BinaryEncoder();
                this.writer = IO.DatumWriter(schema);
                this.fd = fs.openSync(path, this.options.flags); 
                this.writeHeader();
                break;
            }
        }
        return this;
    },
        
    read: function(callback) {
        if (callback)
            callback(null, "The quick brown fox jumped over the lazy dogs");
        return 1;
    },
    
    close: function() {
        if (this.blockCount > 0)
            (function(self) {
                self.writeBlock(self.encoder.buffer(), function(err) {
                    fs.closeSync(self.fd);
                });
            })(this);
    },
    
    end: function() {
        this.close();
    }
}

if (typeof(module.exports) !== 'undefined') {
    module.exports = DataFile;
}