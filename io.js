var _ = require("underscore");
var validator = require('./validator').Validator;

var BinaryDecoder = function(reader) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(reader);
        
    this.reader = reader;
};

BinaryDecoder.prototype = {
    
    readNull: function () {
        // No bytes consumed
        return null;
    },
    
    readBoolean: function () {
        return this.readByte() === 1 ? true : false;
    },
    
    readInt: function () {
        var i;
        var b = this.readByte();
        var n = b & 0x7f;
	    
        for (i = 7; i <= 28 && b > 0x7f; i += 7) {
            b = this.readByte();
            n |= (b & 0x7f) << i;
        }
	    
        if (b > 0x7f) {
            throw "Invalid int encoding.";
        }
	    
        return (n >>> 1) ^ -(n & 1);
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
    
    readFloat: function () {
        var value = this.read32le();
	    
        if (this.strictMode) {    // In strictMode, return the 32 bit
            // float
            return value;
        }
	    
        // Not able to get the floating point back precisely due to
        // noise introduced in JS floating arithmetic
        var sign = ((value >> 31) << 1) + 1;
        var expo = (value & 0x7f800000) >> 23;
        var mant = value & 0x007fffff;
	    
        if (expo === 0) {
            if (mant === 0) {
                return 0;
            }
            expo = -126;
        } else {
            if (expo === 0xff) {
                return mant === 0 ? (sign === 1 ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY) : Number.NaN;
            }
            expo -= 127;
            mant |= 0x00800000;
        }
	    
        return sign * mant * Math.pow(2, expo - 23);
    },

    readDouble : function () {
        var low = this.read32le();
        var high = this.read32le();
	    
        if (this.strictMode) {
            return [low, high];
        }
	    
        var sign = ((high >> 31) << 1) + 1;
        var expo = (high & 0x7ff00000) >> 20;
        var mantHigh = high & 0x000fffff;
        var mant = 0;
	    
        if (expo === 0) {
            if (low === 0 && mantHigh === 0) {
                return 0;
            }
            if (low === 1 && mantHigh === 0) {
                return Number.MIN_VALUE;
            }
            expo = -1022;
        } else {
            if (expo === 0x7ff) {
                if (low === 0 && mantHigh === 0) {
                    return sign === 1 ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
                } else {
                    return Number.NaN;
                }
            }
            if ((high ^ 0x7fefffff) === 0 && (low ^ 0xffffffff) === 0) {
                return Number.MAX_VALUE;
            }
            expo -= 1023;
            mant = 1;
        }
	    
        mant += (low + (high & 0x000fffff) * Math.pow(2, 32)) * Math.pow(2, -52);
        return sign * mant * Math.pow(2, expo);
    },
	
    readFixed : function(len) {
        var result = [];
        var i;
        for (i = 0; i < len; i++) {
            result.push(this.readByte());
        }
        return result;
    },
    
    readBytes : function() {
        var len = this.readLong();
        return this.readFixed(len);
    },
    
    readString : function() {
        return this.utf8Decode(this.readBytes());
    },

    readEnum : function() {
        return this.readInt();
    }
}

var BinaryEncoder = function(writer) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writer);
        
    this.writer = writer;
};

BinaryEncoder.prototype = {   
    
    utf8Encode: function (str) {
        var len = str.length;
        var result = [];
        var code;
        var i;
        for (i = 0; i < len; i++) {
            code = str.charCodeAt(i);
            if (code <= 0x7f) {
                result.push(code);
            } else if (code <= 0x7ff) {                         // 2 bytes
                result.push(0xc0 | (code >>> 6 & 0x1f),
                0x80 | (code & 0x3f));
            } else if (code <= 0xd700 || code >= 0xe000) {      // 3 bytes
                result.push(0xe0 | (code >>> 12 & 0x0f),
                0x80 | (code >>> 6 & 0x3f),
                0x80 | (code & 0x3f));
            } else {                                            // 4 bytes, surrogate pair
                code = (((code - 0xd800) << 10) | (str.charCodeAt(++i) - 0xdc00)) + 0x10000;
                result.push(0xf0 | (code >>> 18 & 0x07),
                0x80 | (code >>> 12 & 0x3f),
                0x80 | (code >>> 6 & 0x3f),
                0x80 | (code & 0x3f));
            }
        }
        return result;
    },
    
    writeNull : function() {
        // Nothing need to write
    },
    
    writeBoolean : function(value) {
        this.writer.writeByte(value ? 1 : 0);
    },
	
    writeInt : function(value) {
        this.writeLong(value);
    },

    writeLong : function (value) {
        var foo = value;
        value = (value << 1) ^ (value >> 63);
        while(value & 0x7f != 0) {
            this.writer.writeByte((value & 0x7f) | 0x80);
            value >>= 7;
        }
        this.writer.writeByte(value);
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
        this.writer.writeByte(out);
        this.writer.writeByte(out >> 8);
        this.writer.writeByte(out >> 16);
        this.writer.writeByte(out >> 24);
    },

    writeDouble: function (value) {
        // To Be Implemented
        throw new Error("not implemented");
    },
        
    writeFixed: function(datum) {
        var i;
        var len = datum.length;
        for (i = 0; i < len; i++) {
            this.writer.writeByte(datum[i]);
        }
    },
    
    writeBytes: function (datum) {
        this.writeLong(datum.length);
        this.writeFixed(datum);
    },
    
    writeString: function(datum) {
        var utf8 = this.utf8Encode(datum);
        this.writeBytes(utf8);
    }
    
}

var DatumReader = function(writersSchema, readersSchema) {
    
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema, readersSchema);
        
    this.writersSchema = writersSchema;
    this.readersSchema = readersSchema;
};

DatumReader.prototype = {

    buffer: "",
    idx: 0,
    
    readByte: function () {
        return this.buffer.charCodeAt(this.idx++);
    },
    
    read: function(decoder){
        if (!this.readersSchema)
            this.readersSchema = this.writersSchema
        this.readData(this.writersSchema, this.readersSchema, decoder)
    },
    
    readData: function(writersSchema, readersSchema, decoder) {
        
        switch(writersSchema.type) {
            case "null":    decoder.readNull(); break;
            case "boolean": decoder.readBoolean(); break;
            case "string":  decoder.readString(); break;
            case "int":     decoder.readInt(); break;
            case "long":    decoder.readLong(); break;
            case "float":   decoder.readFloat(); break;
            case "double":  decoder.readDouble(); break;
            case "bytes":   decoder.readBytes(); break;
            case "fixed":   this.readFixed(writersSchema, readersSchema, decoder); break;
            case "enum":    this.readEnum(writersSchema, readersSchema, decoder); break;
            case "array":   this.readArray(writersSchema, readersSchema, decoder); break;
            case "map":     this.readMap(writersSchema, readersSchema, decoder); break;
            case "union":   this.readUnion(writersSchema, readersSchema, decoder); break;
            case "record":
            case "errors":
            case "request": this.readRecord(writersSchema, readersSchema, decoder); break;
            default:
                throw new Error("Unknown type: " + writersSchema.type);
        }
    },
    
    readFixed: function(writersSchema, readersSchema, decoder) {
        decoder.read(writersSchema.length());
    },
    
    readEnum: function(writersSchema, readersSchema, decoder) {
        var symbolIndex = decoder.readInt();
        var readSymbol = writersSchema.symbols[symbolIndex];
        
        return readSymbol;
    },
    
    readArray: function(writersSchema, readersSchema, decoder) {
        if (datum.length > 0) {
            this.writeLong(datum.length);
            _.each(datum, function(value) {
                writeData(schema, value);
            });
            this.writeLong(0);
        }
    },
    
    readMap: function(schema, datum) {
        readItems = {};
        blockCount = readLong
        if (datum.length > 0) {
            this.writeLong(datum.length);
            _.each(schema, function(value, key) {
                this.writeString(key);
                this.writeData(schema, value);  // Needs fixing
            })
            this.writeLong(0);
        }
    }, 
    
    readUnion: function(writersSchema, readersSchema, decoder) {
        var schemaIndex = decoder.readLong();
        var selectedWritersSchema = writersSchema.schemas[schemaIndex];
        
        return this.readData(selectedWritersSchema, readersSchema, decoder);
    },
    
    readRecord: function(schema, datum) {
        _.each(schema.fields, function(field) {
           this.writeData(field.type, datum[field.name]); 
        });
    }
}

var DatumWriter = function(writersSchema) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema);
        
    this.writersSchema = writersSchema;
};

DatumWriter.prototype = {
    
    buffer: "",
    idx: 0,
        
    writeByte: function(b) {
        this.buffer += String.fromCharCode(b);
        //console.log(b);
        //console.log(this.buffer);
        this.idx++;
    },
    
    write: function(datum, encoder) {
        this.writeData(this.writersSchema, datum, encoder);
    },
    
    writeData: function(writersSchema, datum, encoder) {
        //validator.validate(writersSchema, datum);
        
        switch(writersSchema.type) {
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
        var datumIndex = writersSchema.symbols.index(datum);
        encoder.writeInt(datumIndex);
    },
    
    writeArray: function(writersSchema, datum, encoder) {
        if (datum.length > 0) {
            encoder.writeLong(datum.length);
            _.each(datum, function(value) {
                this.writeData(writersSchema.items, item, encoder);
            });
        }
        encoder.writeLong(0);
    },
    
    writeMap: function(writersSchema, datum, encoder) {
        if (datum.length > 0) {
            encoder.writeLong(_.size(datum));
            _.each(writersSchema, function(value, key) {
                encoder.writeString(key);
                this.writeData(writersSchema, value, encoder);  
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
        var runMe = function(self) {
            //console.log("%j",writersSchema);
            _.each(writersSchema.fields, function(field) {
                self.writeData(typeof(field.type) == 'object' ? field.type : field, 
                               datum[field.name], encoder); 
            });
        }(this);
    }
}

if (typeof(exports) !== 'undefined') {
    exports.BinaryDecoder = BinaryDecoder;
    exports.BinaryEncoder = BinaryEncoder;
    exports.DatumWriter = DatumWriter;
    exports.DatumReader = DatumReader;
}