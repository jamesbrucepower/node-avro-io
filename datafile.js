var fs = require("fs");
var zlib = require("zlib");
var _ = require("underscore");
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
        this.stream.write(this.writer.buffer,'binary');
        this.writer.truncate();
    },
    
    compressData: function(data, callback) {
        switch(this.options.codec) {
            case "null": return data; break;
            case "deflate": {
                zlib.deflateRaw(data, function(err, buffer) {
                    callback(err, buffer);
                });                
                break;
            }
        }
    },
    
    writeBlock: function(data) {
        if (this.blockCount > 0) {
            var bytes = this.compressData(data);
            this.writer.truncate();
            this.writer.writeData(this.blockSchema, this.blockData(bytes), this.encoder);
            console.log(this.blockData(bytes));
            console.log("[%j]",this.writer.buffer);
            this.stream.write(this.writer.buffer,'binary');
            this.writer.truncate();
            this.blockCount = 0;
        }
    },
    
    write: function(data) {
        this.writer.writeData(this.schema, data, this.encoder);
        this.blockCount++;
        
        if (this.writer.buffer.length > this.SYNC_INTERVAL) {
            this.writeBlock(this.writer.buffer);
            this.writer.truncate();
        }
    },
    
    open: function(path, schema, options) {
        this.schema = schema;
        this.writer = IO.DatumWriter(schema);
        this.reader = IO.DatumReader();
        this.encoder = IO.BinaryEncoder(this.writer);
        this.options = _.extend({ codec: "null", flags: 'r', encoding: null, mode: 0666 }, options);

        if (this.VALID_CODECS.indexOf(this.options.codec) == -1)
            throw new Error("Unsupported codec " + this.options.codec);
            
        switch (this.options.flags) {
            case "r": this.stream = fs.createReadStream(path, options); break; 
            case "w": {
                this.stream = fs.createWriteStream(path, options); 
                this.writeHeader(this.options.codec);
                break;
            }
        }
        return this;
    },
        
    read: function(callback) {
        callback(null, "the quick brown fox jumped over the lazy dogs");
    },
    
    close: function() {
        if (this.writer.buffer.length > 0)
            this.writeBlock(this.writer.buffer);
        this.stream.end();
    }
}

if (typeof(module.exports) !== 'undefined') {
    module.exports = DataFile;
}