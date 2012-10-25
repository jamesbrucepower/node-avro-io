var fs = require('fs');

var Avro = function () {
    var Client = function () {};

    Client.prototype = {
        strictMode: false,
        buffer: "",
        idx: 0,
        schemas: {},

        encode: function (schema, datum) {
            //	    this.storeSchemas(schema);
            this.buffer = "";
            this.writeDatum(schema, datum);
            return this.buffer;
        },

        decode: function (schema, buffer) {
            //	    this.storeSchemas(schema);
            this.buffer = buffer;
            return this.readDatum(schema);
        },

        readByte: function () {
            return this.buffer.charCodeAt(this.idx++);
        },

        writeByte: function (b) {
            this.buffer += String.fromCharCode(b);
        },

        storeSchemas: function (schema) {
            if (schema.name !== undefined && schema.fields !== undefined) {
                this.schemas[schema.name] = schema;
		
                console.log("Storing " + schema.name + " as " + schema);
                console.log(schema.fields);

                for (var i in schema.fields) {
                    console.log(schema.fields[i]);

                    this.storeSchemas(schema.fields[i].type);
                }
            }
        },
	    
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
	    
        utf8Decode : function (bytes) {
            var len = bytes.length;
            var result = "";
            var code;
            var i;
            for (i = 0; i < len; i++) {
                if (bytes[i] <= 0x7f) {
                    result += String.fromCharCode(bytes[i]);
                } else if (bytes[i] >= 0xc0) {                                   // Mutlibytes
                    if (bytes[i] < 0xe0) {                                       // 2 bytes
                        code = ((bytes[i++] & 0x1f) << 6) |
                        (bytes[i] & 0x3f);
                    } else if (bytes[i] < 0xf0) {                                // 3 bytes
                        code = ((bytes[i++] & 0x0f) << 12) |
                        ((bytes[i++] & 0x3f) << 6)  |
                        (bytes[i] & 0x3f);
                    } else {                                                     // 4 bytes
                        // turned into two character in JS as surrogate pair
                        code = (((bytes[i++] & 0x07) << 18) |
                        ((bytes[i++] & 0x3f) << 12) |
                        ((bytes[i++] & 0x3f) << 6) |
                        (bytes[i] & 0x3f)) - 0x10000;
                        // High surrogate
                        result += String.fromCharCode((code >>> 10 & 0x3ff) + 0xd800);
                        code = (code & 0x3ff) + 0xdc00;
                    }
                    result += String.fromCharCode(code);
                } // Otherwise it's an invalid UTF-8, skipped.
            }
            return result;
        },

        typeOf: function (value) {
            var s = typeof value;
            if (s === 'object') {
                if (value) {
                    if (value instanceof Array) {
                        s = 'array';
                    }
                } else {
                    s = 'null';
                }
            }
            return s;
        }, // typeOf
    
        ucFirst: function (str) {
            if (str.length <= 1) {
                return str.toUpperCase();
            }
            return str.substring(0, 1).toUpperCase() + str.substring(1);
        },
    
        setStrictMode: function (strict) {
            this.strictMode = strict;
        },

        isInt: function (value){
            if ((parseFloat(value) == parseInt(value)) && !isNaN(value)){
                return true;
            } else {
                return false;
            }
        },

        validate: function (schema, datum) {
            var type = this.typeOf(schema);
            var i;

            switch (type) {
                case "object":
                type = schema.type;
                break;
                case "string":
                type = schema;
                break;
                case "array":
                type = "union";
                break;
                default:
                throw "R:Invalid schema type: " + type;
            }

            //	    console.log("Validating " + datum + " as " + type + " against " + schema);

            switch(type) {
                case "null":
                return datum === undefined;
                case "boolean":
                return datum === true || datum === false;
                case "string":
                case "bytes":
                return datum.constructor === String;
                case "int": // TODO: fix
                case "long":
                return datum.constructor === Number && this.isInt(datum);
                case "double":
                return datum.constructor === Number;
                case "float": // TODO: fix
                return datum.constructor === Number;
                case "enum":
                for (i=0; i < schema.symbols.length; i++) {
                    if (schema.symbols[i] === datum) {
                        return true;
                    }
                }
                return false;
                case "array":
                // TODO: validate integer keys?
                if (datum.constructor == Array) {
                    for (i=0; i < datum.length; i++) {
                        if (!this.validate(schema.items, datum[i])) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
                case "map":
                // TODO: probably won't be an Array, rather Object
                if (datum.constructor == Array) {
                    // TODO: fix - i.constructor always String
                    for (var i in datum) {
                        if (i.constructor !== String) {
                            return false;
                        }

                        if (!this.validate(schema.values, datum[i])) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
                case "union":
                for (i=0; i < schema.length; i++) {
                    if (this.validate(schema[i], datum)) {
                        return true;
                    }
                }
                return false;
                case "record":
                for (var i in schema.fields) {
                    var val = datum[schema.fields[i].name];

                    //		    console.log("Validating " + schema.fields[i].name);
                    if (!this.validate(schema.fields[i].type, val)) {
                        return false;
                    }
                }
                return true;
            }
	    
            return false;
        },

        writeVarInt: function (n) {
            if ((n & ~0x7f) !== 0) {
                this.writeByte(n & 0xff | 0x80);
                n >>>= 7;
                while (n > 0x7f) {
                    this.writeByte(n & 0xff | 0x80);
                    n >>>= 7;
                }
            }
	    
            this.writeByte(n);
        },
	
        writeNull : function () {
            // Nothing need to write
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
        },

        writeBytes : function (bytes, start, len) {
            this.writeLong(len);
            this.writeFixed(bytes, start, len);
        },
	
        writeString : function (str) {
            var utf8 = this.utf8Encode(str);
            this.writeBytes(utf8, 0, utf8.length);
        },

        writeIndex : function (idx) {
            this.writeInt(idx);
        },
	
        writeMapStart : function () {
            // To Be Implemented
        },
	
        writeMapEnd : function () {
            // To Be Implemented
        },
    
        read32le: function () {
            var b;
            var v = 0;
            var i;
            for (i = 0; i < 32; i += 8) {
                b = this.readByte();
                v |= (b << i);
            }
            return v;
        },
	    
        toPaddedHex: function (n) {
            var hex = "";
            var b;
            var i;
            for (i = 0; i < 32; i += 8) {
                b = ((n >>> (i)) & 0x0ff).toString(16);
                hex = (b.length === 1 ? "0" + b : b) + hex;
            }
	    
            return hex;
        },
	    
        // Reads count for array and map
        readCount: function () {
            var count = this.readLong();
            if (count < 0) {
                this.readLong();
                count = -count;
            }
            return count;
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
	
        readFixed : function (len) {
            var result = [];
            var i;
            for (i = 0; i < len; i++) {
                result.push(this.readByte());
            }
            return result;
        },

        readBytes : function () {
            var len = this.readLong();
            return this.readFixed(len);
        },
          
        readString : function () {
            return this.utf8Decode(this.readBytes());
        },

        readEnum : function () {
            return this.readInt();
        },

        readArrayStart : function () {
            return this.readCount();
        },

        arrayNext : function () {
            return this.readCount();
        },

        readMapStart : function () {
            return this.readCount();
        },

        mapNext : function () {
            return this.readCount();
        },
    
        writeDatum: function (schema, datum) {
            // FIXME: refactor crude cut-and-paste from readDatum
            var type;
            var i;
            var result;

            if (schema.name !== undefined) {
                this.schemas[schema.name] = schema;
            }
	    
            type=this.typeOf(schema);
            if (type==="object") {
                type=schema.type;
            }
            else if (type==="string") {
                type=schema;
            }
            else if (type==="array") {
                type="union";
            }
            else if (type==="undefined") {
                throw "W:Undefined schema type " + schema;
            }
            else {
                throw "W:Unrecognized schema type: " 
                + type + schema;
            }
	
            switch (type) {
                // Primitive types
                case "null":
                case "boolean":
                case "int":
                case "long":
                case "float":
                case "double":
                case "bytes":
                case "string":
                return this["write" + this.ucFirst(type)].call(this, datum);
		
                // Complex types
                case "record":
                for (i = 0; i < schema.fields.length; i++) {
                    this.writeDatum(schema.fields[i].type, datum[schema.fields[i].name]);
                }
                return;
		
                case "enum":
                for (i=0; i < schema.symbols.length; i++) {
                    if (schema.symbols[i] == datum) {
                        this.writeInt(i);
                        return;
                    }
                }

                throw "Invalid enum value: " + datum + " expecting: " + schema.symbols;
                return;
		
                case "array":
                if (datum.length > 0) {
                    this.writeLong(datum.length);
		    
                    for (i=0; i < datum.length; i++) {
                        this.writeDatum(schema.items, datum[i]);
                    }
                }

                this.writeLong(0);

                return;
		
                case "map":
                var count = 0;
                for (var k in datum) {
                    if (datum.hasOwnProperty(k)) {
                        ++count;
                    }
                }

                if (count > 0) {
                    this.writeLong(count);
		    
                    for (var k in datum) {
                        this.writeString(k);
                        this.writeDatum(schema.values, datum[k]);
                    }
                }

                this.writeLong(0);

                return;
		
                case "union":

                for (i=0; i < schema.length; i++) {
                    if (this.validate(schema[i], datum)) {
                        this.writeLong(i);
                        return this.writeDatum(schema[i], datum);
                    }
                }

                //		for (var i in datum) {
                    //		    console.log(i + ": " + datum[i]);
                    //		}

                    throw "Invalid value " + JSON.stringify(datum) + " for union: " + schema;
		
                    default:
                    if (this.schemas[type] === undefined) {
                        throw "Unsupported schema type " + type;
                    }

                    return this.writeDatum(this.schemas[type], datum);
                }
            },

            readDatum: function (schema) {
                var type;
                var i;
                var result;
	    
                type= this.typeOf(schema);

                switch (type) {
                    case "object":
                    type = schema.type;
                    break;
                    case "string":
                    type = schema;
                    break;
                    case "array":
                    type = "union";
                    break;
                    default:
                    throw "R:Invalid schema type: " + type;
                }
	    
                //	    console.log("readDatum: " + JSON.stringify(schema) + "; " + type);

                switch (type) {
                    // Primitive types
                    case "null":
                    case "boolean":
                    case "int":
                    case "long":
                    case "float":
                    case "double":
                    case "bytes":
                    case "string":
                    return this["read" + this.ucFirst(type)].apply(this);
		
                    // Complex types
                    case "record":
                    result = {};
                    for (i = 0; i < schema.fields.length; i++) {
                        result[schema.fields[i].name] = this.readDatum(schema.fields[i].type);
                    }
                    return result;
		
                    case "enum":
                    return schema.symbols[this.readEnum()];
		
                    case "array":
                    result = [];
                    i = this.readArrayStart();
                    while (i !== 0) {
                        while (i-- > 0) {
                            result.push(this.readDatum(schema.items));
                        }
                        i = this.arrayNext();
                    }
                    return result;
		
                    case "map":
                    result = {};
                    i = this.readMapStart();
                    while (i !== 0) {
                        while (i-- > 0) {
                            result[this.readDatum("string")] = this.readDatum(schema.values);
                        }
                        i = this.mapNext();
                    }
                    return result;
                    case "union":
                    var idx = this.readLong();
                    //		console.log("here: " + schema + " , " + idx);
                    return this.readDatum(schema[idx]);
		
                    default:
                    console.log("Decoding " + this.schemas[type]);

                    if (this.schemas[type] === undefined) {
                        throw "Unsupported schema type " + type;
                    }

                    return this.readDatum(this.schemas[type]);
                }
            }
        };

        return({
            name: "avro.js",
            version: "0.0.0",

            encode: function (schema, datum) {
                var client = new Client();

                return client.encode(schema, datum);
            },

            decode: function (schema, buffer) {
                var client = new Client();

                return client.decode(schema, buffer);
            },

            validate: function (schema, datum) {
                var client = new Client();
	    
                return client.validate(schema, datum);
            }
        });
    }();

    exports.name = Avro.name;
    exports.version = Avro.version;

    exports.encode = function () {
        return Avro.encode.apply(this, arguments);
    }

    exports.decode = function () {
        return Avro.decode.apply(this, arguments);
    }

    exports.validate = function () {
        return Avro.validate.apply(this, arguments);
    }

