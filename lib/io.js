var _ = require("underscore");
var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var validator = require(__dirname + "/../other/validator").Validator;

var DEFAULT_BUFFER_SIZE = 8192;

var BinaryEncoder = function() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    this.buf = new Buffer(DEFAULT_BUFFER_SIZE);
    this.idx = 0;
};

var BinaryDecoder = function(reader) {
    
    if (reader === 'undefined')
        throw new Error("Must provide a reader");
        
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(reader);
    
    this._reader = reader;
};

BinaryDecoder.prototype = {
            
    readNull: function () {
        // No bytes consumed
        return null;
    },
    
    readByte: function() {
        return this._reader.read(1);
    },
    
    readBoolean: function () {
        return this.readByte() === 1 ? true : false;
    },
    
    readInt: function () {
        return this.readLong();
    },
	
    readLong: function () {
        var b = this.readByte();
        var n = b & 0x7F;
        var shift = 7;

        while ((b & 0x80) != 0) {
            b = this.readByte();
            n |= (b & 0x7F) << shift
            shift += 7
        }
	    
        return (n >> 1) ^ -(n & 1)
    },
    
    readFloat: function() {
        return this._reader.read(4).readFloatLE(0);
    },

    readDouble: function() {
        return this._reader.read(8).readDoubleLE(0);
    },
	
    readFixed: function(len) {
        return this._reader.read(len);
    },
    
    readBytes: function() {
        var len = this.readLong();
        return this.readFixed(len);
    },
    
    readString: function() {
        return this.readBytes().toString();
    },
    
    skipNull: function(){
        return;
    },
    
    skipBoolean: function() {
        this._reader.skip(1);
    },

    skipLong: function() {
        while((this.readByte() & 0x80) != 0) {}
    },
    
    skipFloat: function() {
        return this._reader.skip(4);
    },
    
    skipDouble: function() {
        this._reader.skip(8);
    },
    
    skipBytes: function() {
        var len = this.readLong();
        this._reader.skip(len);
    },
    
    skipString: function() {
        this.skipBytes();
    }    
}

var BinaryEncoder = function() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    this.buf = new Buffer(DEFAULT_BUFFER_SIZE);
    this.idx = 0;
};

BinaryEncoder.prototype = {   
    
    flush: function() {
        this.idx = 0;
    },
    
    buffer: function() {
        var newBuffer = new Buffer(this.idx);
        this.buf.copy(newBuffer, 0, 0, this.idx);
        return newBuffer;
    },
    
    writeByte: function(byte) {
        if (this.idx == DEFAULT_BUFFER_SIZE) {
            throw new Error("Buffer size > " + DEFAULT_BUFFER_SIZE + " not yet implemented");
        }
        this.buf[this.idx++] = byte;
    },
    
    writeNull : function() {
        // This is a no-op
    },
    
    writeBoolean : function(value) {
        this.writeByte(value ? 1 : 0);
    },
	
    writeInt: function(value) {
        this.writeLong(value);
    },

    writeLong: function(value) {
        //console.log("going to encode long %d",value);
        value = (value << 1) ^ (value >> 63);
        //console.log("value to encode %d",value);
        while((value & ~0x7F) !== 0) {
            //console.log("nth byte %d",(value & 0x7f) | 0x80);
            this.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        this.writeByte(value);
    },
    
    writeFloat: function(value) {
        this.buf.writeFloatLE(value, this.idx);
        this.idx += 4;
    },
    
    writeDouble: function (value) {
        // To Be Implemented
        this.buf.writeDoubleLE(value, this.idx);
        this.idx += 8;
    },
        
    // TODO: move to datumwriter
    writeFixed: function(datum) {
        var len = datum.length;
        for (var i = 0; i < len; i++) {
            this.writeByte(datum.charCodeAt(i));
        }
    },

    // TODO: move to datumwriter    
    writeBytes: function(datum) {
        this.writeLong(datum.length);
        if (datum instanceof Buffer) {
            datum.copy(this.buf, this.idx);
            this.idx += datum.length;
        } else 
            this.writeFixed(datum);
    },
    
    writeString: function(datum) {
        var size = Buffer.byteLength(datum);
        this.writeLong(size);
        this.buf.write(datum, this.idx);
        this.idx += size;
    }
    
}

var DatumReader = function(writersSchema, readersSchema) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema, readersSchema);
        
    this.writersSchema = writersSchema;
    this.readersSchema = readersSchema;
};

DatumReader.prototype = {
    
    read: function(decoder){
        if (!this.readersSchema)
            this.readersSchema = this.writersSchema
        return this.readData(this.writersSchema, this.readersSchema, decoder);
    },
    
    getSchemaType: function(schema) {
        if (_.isArray(schema)) {
            return "union";
        } else if (schema.type)
            return schema.type
        else 
            return schema;
    },
    
    readData: function(writersSchema, readersSchema, decoder) {
        
        var schema = this.getSchemaType(writersSchema);
        switch(schema) {
            case "null":    return decoder.readNull(); break;
            case "boolean": return decoder.readBoolean(); break;
            case "string":  return decoder.readString(); break;
            case "int":     return decoder.readInt(); break;
            case "long":    return decoder.readLong(); break;
            case "float":   return decoder.readFloat(); break;
            case "double":  return decoder.readDouble(); break;
            case "bytes":   return decoder.readBytes(); break;
            case "fixed":   return decoder.readFixed(writersSchema.size); break;
            case "enum":    return this.readEnum(writersSchema, readersSchema, decoder); break;
            case "array":   return this.readArray(writersSchema, readersSchema, decoder); break;
            case "map":     return this.readMap(writersSchema, readersSchema, decoder); break;
            case "union":   return this.readUnion(writersSchema, readersSchema, decoder); break;
            case "record":
            case "errors":
            case "request": return this.readRecord(writersSchema, readersSchema, decoder); break;
            default:
                throw new Error("Unknown type: " + schema);
        }
    },
    
    readEnum: function(writersSchema, readersSchema, decoder) {
        var symbolIndex = decoder.readInt();
        var readSymbol = writersSchema.symbols[symbolIndex];
        
        return readSymbol;
    },
    
    readArray: function(writersSchema, readersSchema, decoder) {
        var anArray = [];
        var blockCount = Math.abs(decoder.readLong());
        while(blockCount != 0) {
            for (var i = 0; i < blockCount; i++) {
                anArray.push(this.readData(writersSchema.items, readersSchema.items, decoder));
            }
            blockCount = decoder.readLong();
        }
        return anArray;
    },
    
    readMap: function(writersSchema, readersSchema, decoder) {
        var map = {};
        var blockCount = Math.abs(decoder.readLong());
        while(blockCount != 0) {
            for (var i = 0; i < blockCount; i++) {
                var key = decoder.readString();
                map[key] = this.readData(writersSchema.values, readersSchema.values, decoder);
            }
            blockCount = decoder.readLong();
        }
        return map;
    }, 
    
    readUnion: function(writersSchema, readersSchema, decoder) {
        var schemaIndex = decoder.readLong();
        var selectedWritersSchema = writersSchema[schemaIndex];
        var union = {};
        union[selectedWritersSchema] = this.readData(selectedWritersSchema, readersSchema, decoder);
        
        return union;
    },
    
    readRecord: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var record = {};
        _.each(writersSchema.fields, function(field) {
            record[field.name] = self.readData(field.type, field.type, decoder); 
        });
        return record;
    }
}

var DatumWriter = function(writersSchema) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema);
        
    this.writersSchema = writersSchema;
};

DatumWriter.prototype = {
    
    write: function(datum, encoder) {
        this.writeData(this.writersSchema, datum, encoder);
    },
    
    writeData: function(writersSchema, datum, encoder) {
        //validator.validate(writersSchema, datum);
        
        var schema = writersSchema.type ? writersSchema.type : writersSchema;
        switch(schema) {
            case "null":    encoder.writeNull(datum); break;
            case "boolean": encoder.writeBoolean(datum); break;
            case "string":  encoder.writeString(datum); break;
            case "int":     encoder.writeInt(datum); break;
            case "long":    encoder.writeLong(datum); break;
            case "float":   encoder.writeFloat(datum); break;
            case "double":  encoder.writeDouble(datum); break;
            case "bytes":   encoder.writeBytes(datum); break;
            case "fixed":   encoder.writeFixed(datum); break;
            case "enum":    this.writeEnum(writersSchema, datum, encoder); break;
            case "array":   this.writeArray(writersSchema, datum, encoder); break;
            case "map":     this.writeMap(writersSchema, datum, encoder); break;
            case "union":   this.writeUnion(writersSchema, datum, encoder); break;
            case "record":
            case "errors":
            case "request": this.writeRecord(writersSchema, datum, encoder); break;
            default:
                throw new Error("Unknown type: " + schema + " for data " + datum);
        }
    },
    
    writeEnum: function(writersSchema, datum, encoder) {
        var datumIndex = writersSchema.symbols.indexOf(datum);
        encoder.writeInt(datumIndex);
    },
    
    writeArray: function(writersSchema, datum, encoder) {
        var self = this;
        if (datum.length > 0) {
            encoder.writeLong(datum.length);
            _.each(datum, function(item) {
                self.writeData(writersSchema.items, item, encoder);
            });
        }
        encoder.writeLong(0); 
    },
    
    writeMap: function(writersSchema, datum, encoder) {
        var self = this;
        if (_.size(datum) > 0) {
            encoder.writeLong(_.size(datum));
            _.each(datum, function(value, key) {
                encoder.writeString(key);
                self.writeData(writersSchema.values, value, encoder);  
            })
        }
        encoder.writeLong(0);
    }, 
    
    writeUnion: function(writersSchema, datum, encoder) {
        var schemaIndex = 0;  //FIXME
        
        encoder.writeLong(schemaIndex);
        this.writeData(writersSchema[schemaIndex], datum, encoder);
    },
    
    writeRecord: function(writersSchema, datum, encoder) {
        var self = this;
        //console.log("%j",writersSchema);
        _.each(writersSchema.fields, function(field) {
            //console.log("%j idx=%d",field, encoder.idx);
            self.writeData(typeof(field.type) == 'object' ? field.type : field, 
                           datum[field.name], encoder); 
        });
    }
}

if (typeof(exports) !== 'undefined') {
    exports.BinaryDecoder = BinaryDecoder;
    exports.BinaryEncoder = BinaryEncoder;
    exports.DatumWriter = DatumWriter;
    exports.DatumReader = DatumReader;
}
