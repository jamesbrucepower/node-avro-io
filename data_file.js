var fs = require("fs");
var validator = require('./validator');
var io = require('./io');

var Writer = function(writer, datumWriter, writersSchema) {
    
}

Writer.prototype = {
    generateSyncMarker: function(size) {
        var marker = "";
        for (i = 0; i < size; i++) {
            marker += String.fromCharCode(Math.floor(Math.random() * 0xFF));
        }
        return marker;
    }
}

var DataFile = function(outputFileName, schema) {
    this.outputFileName = outputFileName;
    this.schema = schema;
    this.writer = io.writer;
    this.reader = io.reader;
};

DataFile.prototype = {
    
    VERSION: 1,
    MAGIC: "Obj" + String.fromCharCode(this.VERSION),
    SYNC_SIZE: 16,
    SYNC_INTERVAL: 1000 * this.SYNC_SIZE,
    VALID_CODECS: ["null", "deflate"],
    VALID_ENCODINGS: ["binary"],            // Not used
    
    blockSchema: {
        {"type": "record", "name": "org.apache.avro.Block",
         "fields" : [
           {"name": "objectCount", "type": "long" },
           {"name": "objectSize", "type": "long" },
           {"name": "objects", "type": "bytes" },
           {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
          ]
        }
    },
    
    metaData: function(codec, schema) {
        return {
            "avro.codec": codec,
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
                        "size": this.MAGIC.length
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
        }
    },
    
    writeHeader: function(codec) {
        header = {
            'magic': this.MAGIC,
            'meta': this.metaData(codec, this.schema),
            'sync': this.writer.generateSyncMarker()
        }
        this.writer.writeData(this.metaSchema(), header, this.encoder);
        return this.writer.buffer;
    },
    
    writeData: function(data) {
        
    },
    
    write: function(data, codec, callback) {
        
        this.writeHeader(codec);
        this.writeData(codec);
        callback();
    },
}

module.exports = DataFile;
    
