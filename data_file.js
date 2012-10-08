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
};

DataFile.prototype = {
    
    VERSION: 1,
    MAGIC: "Obj",
    SYNC_SIZE: 16,
    SYNC_INTERVAL: 1000 * this.SYNC_SIZE,
    META_SCHEMA: {"type": "map", "values": "bytes"},
    VALID_CODECS: ['deflate'],
    VALID_ENCODINGS: ['binary'],
    HEADER: {
        "avro.codec": "null",
        "avro.schema": JSON.stringify(this.schema)
    },
    
    writeHeader: function() {
        io.writer.writeString(this.MAGIC);
        io.writer.writeByte(this.VERSION);
        io.writer.writeMap(this.META_SCHEMA, header);
        io.writer.writeSync(this.SYNC_SIZE);
    },
    
    writeData: function(data) {
        
    },
    
    write: function(data, callback) {
        this.writeHeader();
        this.writeData();
        callback();
    },
}

module.exports = DataFile;
    
