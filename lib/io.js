var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';

var _ = require('underscore');
var util = require('util');
var Schema = require(libpath + 'schema').Schema;

var AvroIOError = function() { 
    return new Error('AvroIOError: ' + util.format.apply(null, arguments)); 
};   
	
var BinaryDecoder = function(input) {
    
    if (!input || input == 'undefined')
        throw new AvroIOError('Must provide input');
        
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(input);
    
    this.input(input);
};

BinaryDecoder.prototype = {
            
    input: function(input) {
        if (!input || !input.read || !_.isFunction(input.read))
            throw new AvroIOError("Must provide an input object that implements a read method");
        else 
            this._input = input;
    },
    
    readNull: function () {
        // No bytes consumed
        return null;
    },
    
    readByte: function() {
        return this._input.read(1)[0];
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
        return this._input.read(4).readFloatLE(0);
    },

    readDouble: function() {
        return this._input.read(8).readDoubleLE(0);
    },
	
    readFixed: function(len) {
        return this._input.read(len);
    },
    
    readBytes: function() {
        return this._input.read(this.readLong());
    },
    
    readString: function() {
        return this.readBytes().toString();
    },
    
    skipNull: function(){
        return;
    },
    
    skipBoolean: function() {
        this._input.skip(1);
    },

    skipLong: function() {
        while((this.readByte() & 0x80) != 0) {}
    },
    
    skipFloat: function() {
        return this._input.skip(4);
    },
    
    skipDouble: function() {
        this._input.skip(8);
    },
    
    skipFixed: function(len){
        this._input.skip(len);
    },
    
    skipBytes: function() {
        var len = this.readLong();
        this._input.skip(len);
    },
    
    skipString: function() {
        this.skipBytes();
    }    
}
    
var BinaryEncoder = function(output) {
    
    if (!output || output === 'undefined')
        throw new AvroIOError("Must provide an output object");
		
    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(output);
        
    this.output(output);
};

BinaryEncoder.prototype = {   
    
	output: function(output) {
    	if (!output || !output.write || !_.isFunction(output.write))
    		throw new AvroIOError("Must provide an output object that implements the write method");
        else
            this._output = output;
	},
	
    writeByte: function(value){
        this._output.write(value);
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
        value = (value << 1) ^ (value >> 63);
        while((value & ~0x7F) !== 0) {
            this.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        this.writeByte(value);
    },
    
    writeFloat: function(value) {
        var floatBuffer = new Buffer(4);
        floatBuffer.writeFloatLE(value, 0);
        this._output.write(floatBuffer);
    },
    
    writeDouble: function(value) {
        var doubleBuffer = new Buffer(8);
        doubleBuffer.writeDoubleLE(value, 0);
        this._output.write(doubleBuffer);
    },
        
    writeBytes: function(datum) {
        if (!Buffer.isBuffer(datum) && !_.isArray(datum))
            throw new AvroIOError("must pass in an array of byte values or a buffer");
            
        this.writeLong(datum.length);
        this._output.write(datum);
    },
    
    writeString: function(datum) {
        if (!_.isString(datum))
            throw new AvroIOError("argument must be a string but was %s(%s)", datum, typeof(datum));
        
        var size = Buffer.byteLength(datum);
        var stringBuffer = new Buffer(size);
        stringBuffer.write(datum);
        this.writeLong(size);
        this._output.write(stringBuffer);
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
        if (!this.readersSchema) this.readersSchema = this.writersSchema;
        return this.readData(this.writersSchema, this.readersSchema, decoder);
    },
    
    readData: function(writersSchema, readersSchema, decoder) {
        
        if (!(writersSchema instanceof Avro.Schema))
            throw new AvroIOError("writersSchema is not a valid schema object");
        
        if (readersSchema && !(readersSchema instanceof Avro.Schema))
            throw new AvroIOError("readersSchema is not a valid schema object");
        
        if (!readersSchema) readersSchema = writersSchema;
            
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
                throw new AvroIOError("Unknown type: %j", writersSchema);
        }
    },
    
    skipData: function(writersSchema, decoder) {
        
        if (!(writersSchema instanceof Avro.Schema))
            throw new AvroIOError("writersSchema is not a valid schema object");
            
        switch(writersSchema.type) {
            case "null":    return decoder.skipNull(); break;
            case "boolean": return decoder.skipBoolean(); break;
            case "string":  return decoder.skipString(); break;
            case "int":     return decoder.skipLong(); break;
            case "long":    return decoder.skipLong(); break;
            case "float":   return decoder.skipFloat(); break;
            case "double":  return decoder.skipDouble(); break;
            case "bytes":   return decoder.skipBytes(); break;
            case "fixed":   return decoder.skipFixed(writersSchema.size); break;
            case "enum":    return this.skipEnum(writersSchema, decoder); break;
            case "array":   return this.skipArray(writersSchema, decoder); break;
            case "map":     return this.skipMap(writersSchema, decoder); break;
            case "union":   return this.skipUnion(writersSchema, decoder); break;
            case "record":
            case "errors":
            case "request": return this.skipRecord(writersSchema, decoder); break;
            default:
                throw new AvroIOError("Unknown type: %j", writersSchema);
        }
    },
    
    readEnum: function(writersSchema, readersSchema, decoder) {
        var symbolIndex = Math.abs(decoder.readInt());
        if (symbolIndex > 0 && symbolIndex < writersSchema.symbols.length)
            return writersSchema.symbols[symbolIndex];
    },
    
    skipEnum: function(writersSchema, decoder) {
        decoder.skipLong();
    },
    
    readArray: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var anArray = [];
        this.readBlocks(decoder, function() {
            anArray.push(self.readData(writersSchema.items, readersSchema.items, decoder));
        });
        return anArray;
    },
    
    skipArray: function(writersSchema, decoder) {
        var self = this;
        this.skipBlocks(decoder, function() {
            self.skipData(writersSchema.items, decoder);
        })
    },
    
    readMap: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var map = {};
        this.readBlocks(decoder, function() {
            var key = decoder.readString();
            var value = self.readData(writersSchema.values, readersSchema.values, decoder); 
            map[key] = value;
        });
        return map;
    }, 
    
    skipMap: function(writersSchema, decoder) {
        var self = this;
        this.skipBlocks(decoder, function() {
            decoder.skipString();
            self.skipData(writersSchema.values, decoder);
        })
    },

    readUnion: function(writersSchema, readersSchema, decoder) {
        var schemaIndex = decoder.readLong(); 
        if (schemaIndex < 0 || schemaIndex >= writersSchema.schemas.length) {
            throw new AvroIOError("Union of size %d is out of bounds at %d", writersSchema.schemas.length, schemaIndex);   
        }
        var selectedWritersSchema = writersSchema.schemas[schemaIndex];
        var union = {};
        union[selectedWritersSchema.name || selectedWritersSchema.type] = this.readData(selectedWritersSchema, readersSchema.schemas[schemaIndex], decoder);
        
        return union;
    },
    
    skipUnion: function(writersSchema, decoder) {
        var index = decoder.readLong();
        this.skipData(writersSchema.schemas[index], decoder)
    },
    
    readRecord: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var record = {};
        _.each(writersSchema.fields, function(field) {
            var readersField = readersSchema.fieldsHash[field.name];
            console.error(readersSchema.fieldsHash);
            if (readersField) {
                record[field.name] = self.readData(field.type, readersField.type, decoder); 
            } else {
                self.skipData(field.type, decoder);
            }
        });
        return record;
    },
    
    skipRecord: function(writersSchema, decoder) {
        var self = this;
        _.each(writersSchema.fields, function(field) {
            self.skipData(field.type, decoder);
        });
    },
    
    _iterateBlocks: function(decoder, iterations, lambda){
        var count = decoder.readLong();
        while(count) { 
            if (count < 0) {
                count = -count;
                iterations();
            }
            while(count--) lambda();
            count = decoder.readLong();
        }
    },
    
    readBlocks: function(decoder, lambda) {
        this._iterateBlocks(decoder, function() { decoder.readLong() }, lambda);
    },
    
    skipBlocks: function(decoder, lambda) {
        this._iterateBlocks(decoder, function() { decoder.skipFixed(decoder.readLong()) }, lambda);
    }
}

var DatumWriter = function(writersSchema) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema);
        
    if (writersSchema && !(writersSchema instanceof Avro.Schema))
        throw new AvroIOError("writersSchema should be an instance of Schema");
        
    this.writersSchema = writersSchema;
};

DatumWriter.prototype = {
    
    write: function(datum, encoder) {
        this.writeData(this.writersSchema, datum, encoder);
    },
    
    writeData: function(writersSchema, datum, encoder) {
        if (!(writersSchema instanceof Avro.Schema))
            throw new AvroIOError("writersSchema is not a valid schema object, it is %j", writersSchema);

        writersSchema.validate(writersSchema, datum);
        
        switch(writersSchema.type) {
            case "null":    encoder.writeNull(datum); break;
            case "boolean": encoder.writeBoolean(datum); break;
            case "string":  encoder.writeString(datum); break;
            case "int":     encoder.writeInt(datum); break;
            case "long":    encoder.writeLong(datum); break;
            case "float":   encoder.writeFloat(datum); break;
            case "double":  encoder.writeDouble(datum); break;
            case "bytes":   encoder.writeBytes(datum); break;
            case "fixed":   this.writeFixed(writersSchema, datum, encoder); break;
            case "enum":    this.writeEnum(writersSchema, datum, encoder); break;
            case "array":   this.writeArray(writersSchema, datum, encoder); break;
            case "map":     this.writeMap(writersSchema, datum, encoder); break;
            case "union":   this.writeUnion(writersSchema, datum, encoder); break;
            case "record":
            case "errors":
            case "request": this.writeRecord(writersSchema, datum, encoder); break;
            default:
                throw new AvroIOError("Unknown type: %j for data %j", writersSchema, datum);
        }
    },
    
    writeFixed: function(writersSchema, datum, encoder) {
        var len = datum.length;
        for (var i = 0; i < len; i++) {
            encoder.writeByte(datum.charCodeAt(i));
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
        var schemaIndex = -1;    
        var key;
        for (var i = 0; i < writersSchema.schemas.length; i++) {
            if (writersSchema.schemas[i].type === _.keys(datum)[0] ||
                writersSchema.schemas[i].name === _.keys(datum)[0]) {
                schemaIndex = i;
                key = _.keys(datum)[0];
            }
        }
        
        if (schemaIndex < 0) {
            throw new AvroIOError("No schema found for data %j", datum);
        } else {
            encoder.writeLong(schemaIndex);
            this.writeData(writersSchema.schemas[schemaIndex], datum[key], encoder);
        }
    },
    
    writeRecord: function(writersSchema, datum, encoder) {
        var self = this;
        _.each(writersSchema.fields, function(field) {
            self.writeData(field.type, datum[field.name], encoder); 
        });
    }
}

if (!_.isUndefined(exports)) {
    exports.BinaryDecoder = BinaryDecoder;
    exports.BinaryEncoder = BinaryEncoder;
    exports.DatumWriter = DatumWriter;
    exports.DatumReader = DatumReader;
}
