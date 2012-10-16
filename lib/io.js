var _ = require("underscore");
var validator = require(__dirname + "/../lib/validator").Validator;

var DEFAULT_BUFFER_SIZE = 2048;

var BinaryEncoder = function() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    this.buf = new Buffer(DEFAULT_BUFFER_SIZE);
    this.idx = 0;
};

var BinaryDecoder = function() {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee();
        
    this.buf = new Buffer(DEFAULT_BUFFER_SIZE);
    this.idx = 0;
};

BinaryDecoder.prototype = {
    
    setBuffer: function(buffer) {
        if (!buffer instanceof Buffer)
            throw new Error("Must pass in an instance of a Buffer");
            
        this.buf = buffer;
        this.idx = 0;
    },
    
    readNull: function () {
        // No bytes consumed
        return null;
    },
    
    readByte: function() {
        return this.buf[this.idx++];
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
        this.idx += 4;
        return this.buf.readFloatLE(this.idx-4);
    },

    readDouble: function() {
        this.idx += 8;
        return this.buf.readDoubleLE(this.idx-8);
    },
	
    readFixed: function(len) {
        this.idx += len;
        return this.buf.slice(this.idx - len, this.idx);
    },
    
    readBytes: function() {
        var len = this.readLong();
        return this.readFixed(len);
    },
    
    readString: function() {
        var sBuffer = this.readBytes();
        return sBuffer.toString();
    },
    
    skipNull: function(){
        return null;
    },
    
    skipBoolean: function(){
        return this.idx++;
    },

    skipLong: function(){
        while((this.readByte() & 0x80) != 0) {}
    },
    
    skipFloat: function(){
        this.idx += 4;
    },
    
    skipDouble: function(){
        this.idx += 8;
    },
    
    skipBytes: function(){
        var len = this.readLong();
        this.idx += len;
    },
    
    skipString: function(){
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
        // Nothing need to write
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

    writeFloat : function (f) {
        var out=0.0;
        // attrib: http://stackoverflow.com/questions/3077718/converting-a-decimal-value-to-a-32bit-floating-point-hexadecimal
        var NAN_BITS = 0|0x7FC00000;
        var INF_BITS = 0|0x7F800000;
        var ZERO_BITS = 0|0x00000000;
        var SIGN_MASK = 0|0x80000000;
        var EXP_MASK = 0|0x7F800000;
        var MANT_MASK = 0|0x007FFFFF;
        var MANT_MAX = Math.pow(2.0, 23) - 1.0;
	    
        var fabs = Math.abs(f);
        var hasSign = f < 0.0 || (f === 0.0 && 1.0 / f < 0);
        var signBits = hasSign ? SIGN_MASK : 0;
        if (isNaN(f)) {
            out=NAN_BITS;
        }
        else if (fabs === Number.POSITIVE_INFINITY) {
            out=signBits | INF_BITS;
        }
        else {
            var exp = 0, x = fabs;
            while (x >= 2.0 && exp <= 127) {
                exp++;
                x /= 2.0;
            }
            while (x < 1.0 && exp >= -126) {
                exp--;
                x *= 2.0;
            }
		
            var biasedExp = exp + 127;
		
            if (biasedExp === 255) {
                out=signBit | INF_BITS;
            }
		
            var mantissa=0.0;
            if (biasedExp === 0) {
                mantissa = x * Math.pow(2.0, 23) / 2.0;
            } 
            else {
                mantissa = x * Math.pow(2.0, 23) - Math.pow(2.0, 23);
            }
		
            var expBits = (biasedExp << 23) & EXP_MASK;
            var mantissaBits = mantissa & MANT_MASK;
		
            out = signBits | expBits | mantissaBits;
        }
	    
        // FIXME: endian consideration necessary?
        this.writeByte(out);
        this.writeByte(out >> 8);
        this.writeByte(out >> 16);
        this.writeByte(out >> 24);
    },

    writeDouble: function (value) {
        // To Be Implemented
        throw new Error("not implemented");
    },
        
    writeFixed: function(datum) {
        var len = datum.length;
        for (var i = 0; i < len; i++) {
            this.writeByte(datum.charCodeAt(i));
        }
    },
    
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
        this.readData(this.writersSchema, this.readersSchema, decoder)
    },
    
    readData: function(writersSchema, readersSchema, decoder) {
        
        switch(writersSchema.type) {
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
                throw new Error("Unknown type: " + writersSchema.type);
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
                anArray.push(this.readData(writersSchema.values, readersSchema.values, decoder));
            }
            blockCount = decoder.readLong();
        }
        return anArray;
    },
    
    readMap: function(writersSchema, readersSchema, decoder) {
        var map = {};
        var blockCount = Math.abs(this.decoder.readLong());
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
        var selectedWritersSchema = writersSchema.schemas[schemaIndex];
        var union = {};
        union[selectedWritersSchema] = this.readData(selectedWritersSchema, readersSchema, decoder);
        
        return union;
    },
    
    readRecord: function(writersSchema, readersSchema, decoder) {
        var record = {};
        _.each(writersSchema.fields, function(field) {
            if (read)
           this.writeData(field.type, datum[field.name]); 
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
        
        //console.log("%j:%j", datum, writersSchema)
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
                throw new Error("Unknown type: " + writersSchema.type + " for data " + datum + ", schema was " + writersSchema);
        }
    },
    
    writeEnum: function(writersSchema, datum, encoder) {
        var datumIndex = writersSchema.symbols.indexOf(datum);
        encoder.writeInt(datumIndex);
    },
    
    writeArray: function(writersSchema, datum, encoder) {
        (function(self) {
            if (datum.length > 0) {
                encoder.writeLong(datum.length);
                _.each(datum, function(item) {
                    self.writeData(writersSchema.items, item, encoder);
                });
            }
            encoder.writeLong(0);
        })(this);  
    },
    
    writeMap: function(writersSchema, datum, encoder) {
        (function(self, schema) {
            if (_.size(datum) > 0) {
                encoder.writeLong(_.size(datum));
                _.each(datum, function(value, key) {
                    encoder.writeString(key);
                    self.writeData(schema.values, value, encoder);  
                })
            }
            encoder.writeLong(0);
        })(this, writersSchema);
    }, 
    
    writeUnion: function(writersSchema, datum, encoder) {
        var schemaIndex = 0;  //FIXME
        
        encoder.writeLong(schemaIndex);
        this.writeData(writersSchema[schemaIndex], datum, encoder);
    },
    
    writeRecord: function(writersSchema, datum, encoder) {
        (function(self) {
            //console.log("%j",writersSchema);
            _.each(writersSchema.fields, function(field) {
                //console.log("%j idx=%d",field, encoder.idx);
                self.writeData(typeof(field.type) == 'object' ? field.type : field, 
                               datum[field.name], encoder); 
            });
        })(this);
    }
}

if (typeof(exports) !== 'undefined') {
    exports.BinaryDecoder = BinaryDecoder;
    exports.BinaryEncoder = BinaryEncoder;
    exports.DatumWriter = DatumWriter;
    exports.DatumReader = DatumReader;
}
