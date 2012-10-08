var fs = require("fs");
var validator = require('./validator');
var io = require('./io');

var DataFile = function(schema) {
    this.schema = schema;
};

DataFile.prototype = {
    
    VERSION: 1,
    MAGIC: "Obj",
    MAGIC_SIZE: this.MAGIC.length + 1,
    SYNC_SIZE: 16,
    SYNC_INTERVAL: 1000 * this.SYNC_SIZE,
    META_SCHEMA = {"type": "map", "values": "bytes"},
    VALID_CODECS: ['deflate'],
    VALID_ENCODINGS: ['binary'],

    header = {
        "avro.codec": "null",
        "avro.schema": JSON.stringify(this.schema);
    },

    writeHeader: function() {
        io.writeString(MAGIC);
        io.writeByte(this.VERSION);
        io.writeMap(this.META_SCHEMA, header);
    },
    
    writeData: function(data) {
        
    },
    
    writeFile: function(filename, data) {
        
        this.writeHeader;
    },
}

module.exports = DataFile;
    
