var fs = require("fs");
var zlib = require("zlib");
var validator = require("./validator").Validator;
var IO = require("./io");

var DataFile = function() {
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
};

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
               {"name": "objectSize", "type": "long" },
               {"name": "objects", "type": "bytes" },
               {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": this.SYNC_SIZE}}
            ]
    },
    
    blockData: function(datum) {
        return {
            "objectCount": this.blockCount,
            "objectSize": datum.length,
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
    
    writeHeader: function(codec) {
        this.syncMarker = this.generateSyncMarker(this.SYNC_SIZE);
        var avroHeader = {
            'magic': this.magic(),
            'meta': this.metaData(codec, this.schema),
            'sync': this.syncMarker
        };
        this.writer.writeData(this.metaSchema(), avroHeader, this.encoder);
    },
    
    writeBlock: function(data) {
        this.writer.writeData(this.blockSchema, this.blockData(data));
        this.blockCount++;
    },
    
    writeData: function(codec, data, callback) {
        var compressed = "";
        //console.error("before %d bytes", data.length);
        this.writer.writeData(this.schema, data, this.encoder);
        callback();
        switch (codec) {
            case "null": compressed = data; break;
            case "deflate": {
                zlib.deflate(data, function(err, buffer) {
                    compressed = buffer;
                    //console.error("after %d bytes", compressed.length);
                    callback(null, compressed);
                });
                break;
            }
        }
    },
    
    open: function(path, flags, schema) {
        this.path = path;
        this.flags = flags;
        this.schema = schema;
        this.writer = IO.DatumWriter(schema);
        this.reader = IO.DatumReader();
        this.encoder = IO.BinaryEncoder(this.writer);
        
        return this;
    },
    
    write: function(data, codec, callback) {
        
        (function(self) {
            if (codec && self.VALID_CODECS.indexOf(codec) == -1)
                throw new Error("Unsupported codec %s", codec);
            
            self.writeHeader(codec);
            self.writer.clear();
            //console.log("%j", self.writer.buffer);
            self.writeData(codec, data, function(err, data) {
                //console.log("%j", self.writer.buffer);
                
                fs.writeFileSync(self.path, self.writer.buffer, 'binary');
                callback(err);            
            });
        })(this);
    },
    
    read: function(callback) {
        callback(null, "the quick brown fox jumped over the lazy dogs");
    }
}

if (typeof(module.exports) !== 'undefined') {
    module.exports = DataFile;
}