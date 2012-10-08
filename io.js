var _ = require("underscore");

var BinaryDecoder = function(reader) {
    this.reader = reader;
};

BinaryDecoder.prototype = {
    
    buffer: "",
    
    readByte: function () {
        return this.buffer.charCodeAt(this.idx++);
    },
    
    readNull : function () {
        // No bytes consumed
        return null;
    },
    
    readBoolean : function () {
        return this.readByte() === 1 ? true : false;
    },
    
    readInt : function () {
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
	
    readLong : function () {
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
    
    readFloat : function () {
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
    },
}

var BinaryEncoder = function(writer) {
    this.writer = writer;
};

BinaryEncoder.prototype = {   
    
    writeNull : function () {
        // Nothing need to write
    },

    writeByte: function (b) {
        this.buffer += String.fromCharCode(b);
    },
    
    writeBoolean : function (value) {
        this.writeByte(value ? 1 : 0);
    },
	
    writeInt : function (value) {
        var n = (value << 1) ^ (value >> 31);
        this.writeVarInt(n);
    },

    writeLong : function (value) {
        var foo = value;
        value = (value << 1) ^ (value >> 63);
        while(value & 0x7f != 0) {
            this.writeByte((value & 0x7f) | 0x80);
            value >>= 7;
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

    writeDouble : function (value) {
        // To Be Implemented
    },

    writeFixed : function (bytes, start, len) {
        var i;
        var end = start + len;
        for (i = start; i < end; i++) {
            this.writeByte(bytes[i]);
        }
    }
}

var Reader = function() {};

Reader.prototype = {
    
    readArray: function(schema, datum) {
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
    
    readUnion: function(schema, datum) {
        
    },
    
    readRecord: function(schema, datum) {
        _.each(schema.fields, function(field) {
           this.writeData(field.type, datum[field.name]); 
        });
    }
}

var Writer = function() {};

Writer.prototype = {
    
    buffer: "",
        
    writeData: function(schema, datum) {
        validator.validate(schema);
        
        switch(schema.type) {
            case "null":    this.writeNull(datum); break;
            case "boolean": this.writeBoolean(datum); break;
            case "string":  this.writeString(datum); break;
            case "int":     this.writeInt(datum); break;
            case "long":    this.writeLong(datum); break;
            case "float":   this.writeFloat(datum); break;
            case "double":  this.writeDouble(datum); break;
            case "bytes":   this.writeBytes(datum); break;
            case "fixed":   this.writeFixed(datum); break;
            case "enum":    this.writeEnum(stringchema, datum); break;
            case "array":   this.writeArray(schema, datum); break;
            case "map":     this.writeMap(schema, datum); break;
            case "union":   this.writeUnion(schema, datum); break;
            case "record":
            case "errors":
            case "request": this.writeRecord(schema, datum); break;
            default:
                throw new Error("Unknown type: " + schema.type);
        }
    },
    
    writeString : function (str) {
        var utf8 = this.utf8Encode(str);
        this.writeBytes(utf8, 0, utf8.length);
    },
    
    writeFixed: function(schema, datum) {
        
    },
    
    writeEnum: function(schema, datum) {
        
    },
    
    writeArray: function(schema, datum) {
        if (datum.length > 0) {
            this.writeLong(datum.length);
            _.each(datum, function(value) {
                writeData(schema, value);
            });
            this.writeLong(0);
        }
    },
    
    writeMap: function(schema, datum) {
        if (datum.length > 0) {
            this.writeLong(datum.length);
            _.each(schema, function(value, key) {
                this.writeString(key);
                this.writeData(schema, value);  // Needs fixing
            })
            this.writeLong(0);
        }
    }, 
    
    writeUnion: function(schema, datum) {
        
    },
    
    writeRecord: function(schema, datum) {
        _.each(schema.fields, function(field) {
           this.writeData(field.type, datum[field.name]); 
        });
    }
}

var IO = function() {}

IO.prototype = {
    
    reader: new Reader(),
    writer: new Writer()
    
}

module.exports = IO;