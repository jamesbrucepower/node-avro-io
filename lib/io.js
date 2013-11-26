var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/';

var _ = require('underscore');
var util = require('util');
var Avro = require(libpath + 'schema');
var AvroErrors = require(libpath + 'errors.js');

var BinaryDecoder = function(input) {

    if (!input || input == 'undefined')
        throw new AvroErrors.IOError('Must provide input');

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(input);

    this.input(input);
};

BinaryDecoder.prototype = {

    input: function(input) {
        if (!input || !input.read || !_.isFunction(input.read))
            throw new AvroErrors.IOError("Must provide an input object that implements a read method");
        else
            this._input = input;
    },

    readNull: function () {
        // No bytes consumed
        return null;
    },

    readByte: function() {
        return this._input.read(1);
    },

    readBoolean: function () {
        var bool = this.readByte();
        if (bool instanceof AvroErrors.BlockDelayReadError)
            return bool;
        else
            return bool === 1 ? true : false;
    },

    readInt: function () {
        return this.readLong();
    },

    readLong: function () {
        var oldOffset = this._input.offset;
        var b = this.readByte();
        if (b instanceof AvroErrors.BlockDelayReadError) return b;

        var n = b & 0x7F;
        var shift = 7;

        while ((b & 0x80) != 0) {
            b = this.readByte();
            if (b instanceof AvroErrors.BlockDelayReadError) {
                //console.error("offset now is %d, old was %d, shift is %d", this._input.offset, oldOffset, shift);
                this._input.skip(oldOffset - this._input.offset);
                return b;
            }
            n |= (b & 0x7F) << shift
            shift += 7
        }

        return (n >> 1) ^ -(n & 1);
    },

    readFloat: function() {
        var bytes = this._input.read(4);
        if (bytes instanceof AvroErrors.BlockDelayReadError)
            return bytes;
        else
            return bytes.readFloatLE(0);
    },

    readDouble: function() {
        var bytes = this._input.read(8);
        if (bytes instanceof AvroErrors.BlockDelayReadError)
            return bytes;
        else
            return bytes.readDoubleLE(0);
    },

    readFixed: function(len) {
        if (len < 1)
            throw new AvroErrors.IOError("readFixed only read %d", len);
        else
            return this._input.read(len);
    },

    readBytes: function() {
        var oldOffset = this._input.offset;
        var len = this.readLong();
        //console.error("I want to read %d bytes, from %d (%d)", len, oldOffset, this._input.length);
        if (len && len > 0) {
            var bytes = this.readFixed(len);
            if (bytes instanceof AvroErrors.BlockDelayReadError)
                this._input.skip(oldOffset - this._input.offset);
            return bytes;
        } else
            return new Buffer(0);
    },

    readString: function() {
        var bytes = this.readBytes();
        if (bytes instanceof AvroErrors.BlockDelayReadError)
            return bytes;
        else {
            if (Buffer.isBuffer(bytes))
                return bytes.toString();
            else 
                return String.fromCharCode(bytes);
        }
    },

    skipNull: function(){
        return;
    },

    skipBoolean: function() {
        return this._input.skip(1);
    },

    skipLong: function() {
        while((this.readByte() & 0x80) != 0) {}
    },

    skipFloat: function() {
        return this._input.skip(4);
    },

    skipDouble: function() {
        return this._input.skip(8);
    },

    skipFixed: function(len){
        return this._input.skip(len);
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
        throw new AvroErrors.IOError("Must provide an output object");

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(output);

    this.output(output);
};

BinaryEncoder.prototype = {

    output: function(output) {
        if (!output || !output.write || !_.isFunction(output.write))
            throw new AvroErrors.IOError("Must provide an output object that implements the write method");
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
            throw new AvroErrors.IOError("must pass in an array of byte values or a buffer");

        this.writeLong(datum.length);
        this._output.write(datum);
    },

    writeString: function(datum) {
        if (!_.isString(datum))
            throw new AvroErrors.IOError("argument must be a string but was %s(%s)", datum, typeof(datum));

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
            throw new AvroErrors.IOError("writersSchema is not a valid schema object");

        if (readersSchema && !(readersSchema instanceof Avro.Schema))
            throw new AvroErrors.IOError("readersSchema is not a valid schema object");

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
                throw new AvroErrors.IOError("Unknown type: %j", writersSchema);
        }
    },

    skipData: function(writersSchema, decoder) {

        if (!(writersSchema instanceof Avro.Schema))
            throw new AvroErrors.IOError("writersSchema is not a valid schema object");

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
                throw new AvroErrors.IOError("Unknown type: %j", writersSchema);
        }
    },

    readEnum: function(writersSchema, readersSchema, decoder) {
        var anEnum = decoder.readInt();
        if (anEnum instanceof AvroErrors.BlockDelayReadError)
            return anEnum;
        var symbolIndex = Math.abs(anEnum);
        if (symbolIndex > 0 && symbolIndex < writersSchema.symbols.length)
            return writersSchema.symbols[symbolIndex];
    },

    skipEnum: function(writersSchema, decoder) {
        return decoder.skipLong();
    },

    readArray: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var anArray = [];
        this.readBlocks(decoder, function() {
            anArray.push(self.readData(writersSchema.items, readersSchema.items, decoder));
        })
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
        var block = this.readBlocks(decoder, function() {
            if (map instanceof AvroErrors.BlockDelayReadError) return;
            var key = decoder.readString();
            var value = self.readData(writersSchema.values, readersSchema.values, decoder);
            if (key instanceof AvroErrors.BlockDelayReadError)
                map = key;
            else if (value instanceof AvroErrors.BlockDelayReadError) {
                map = value;
            } else
                map[key] = value;
        });
        if (block instanceof AvroErrors.BlockDelayReadError)
            return block;
        else
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
        var oldOffset = decoder._input.offset;
        var schemaIndex = decoder.readLong();
        if (schemaIndex instanceof AvroErrors.BlockDelayReadError) return schemaIndex;
        if (schemaIndex < 0 || schemaIndex >= writersSchema.schemas.length) {
            throw new AvroErrors.IOError("Union %j is out of bounds for %d, %d, %d", writersSchema, schemaIndex, decoder._input.offset, decoder._input.length);
        }
        var selectedWritersSchema = writersSchema.schemas[schemaIndex];
        var union = {};
        var data = this.readData(selectedWritersSchema, readersSchema.schemas[schemaIndex], decoder);
        if (data instanceof AvroErrors.BlockDelayReadError) {
            decoder._input.skip(oldOffset - decoder._input.offset);
            return data;
        }
        union = data;

        return union;
    },

    skipUnion: function(writersSchema, decoder) {
        var index = decoder.readLong();
        if (index === null)
            return null
        else
            return this.skipData(writersSchema.schemas[index], decoder)
    },

    readRecord: function(writersSchema, readersSchema, decoder) {
        var self = this;
        var record = {};
        var oldOffset = decoder._input.offset;
        for (var fieldIdx in writersSchema.fields) {
            var field = writersSchema.fields[fieldIdx];
            var readersField = readersSchema.fieldsHash[field.name];
            if (readersField) {
                var data = self.readData(field.type, readersField.type, decoder);
                if (data instanceof AvroErrors.BlockDelayReadError) {
                    decoder._input.skip(oldOffset - decoder._input.offset);
                    return data;
                } else
                    record[field.name] = data;
            } else {
                console.error('SKIPPING');
                self.skipData(field.type, decoder);
            }
        };
        return record;
    },

    skipRecord: function(writersSchema, decoder) {
        var self = this;
        _.each(writersSchema.fields, function(field) {
            self.skipData(field.type, decoder);
        });
    },

    _iterateBlocks: function(decoder, iteration, lambda) {
        var oldOffset = decoder._input.offset;
        var count = decoder.readLong();
        if (count instanceof AvroErrors.BlockDelayReadError) return count;
        while(count) {
            if (count < 0) {
                count = -count;
                var output = iteration();
                if (output instanceof AvroErrors.BlockDelayReadError) {
                    decoder._input.skip(oldOffset - decoder._input.offset);
                    return output;
                }
            }
            while(count--) lambda();
            count = decoder.readLong();
            if (count instanceof AvroErrors.BlockDelayReadError) {
                decoder._input.skip(oldOffset - decoder._input.offset);
                return count;
            }
        }
    },

    readBlocks: function(decoder, lambda) {
        return this._iterateBlocks(decoder, function() { return decoder.readLong(); }, lambda);
    },

    skipBlocks: function(decoder, lambda) {
        return this._iterateBlocks(decoder, function() { decoder.skipFixed(decoder.readLong()) }, lambda);
    }
}

var DatumWriter = function(writersSchema) {

    if ((this instanceof arguments.callee) === false)
        return new arguments.callee(writersSchema);

    if (writersSchema && !(writersSchema instanceof Avro.Schema))
        throw new AvroErrors.IOError("writersSchema should be an instance of Schema");

    this.writersSchema = writersSchema;
};

DatumWriter.prototype = {

    write: function(datum, encoder) {
        this.writeData(this.writersSchema, datum, encoder);
    },

    writeData: function(writersSchema, datum, encoder) {
        if (!(writersSchema instanceof Avro.Schema))
            throw new AvroErrors.IOError("writersSchema is not a valid schema object, it is %j", writersSchema);

        if (!writersSchema.validate(writersSchema.type, datum)) {
            throw new AvroErrors.IOError("Data %j is not valid type of %j", datum, writersSchema.type);
        }

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
                throw new AvroErrors.IOError("Unknown type: %j for data %j", writersSchema, datum);
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

        for (var i = 0; i < writersSchema.schemas.length; i++) {
            if (writersSchema.schemas[i].type === 'record' && writersSchema.schemas[i].name == _.keys(datum)[0]) {
                schemaIndex = i;
                datum = datum[_.keys(datum)[0]];
                break;
            } else if (writersSchema.isPrimitive(writersSchema.schemas[i].type) && writersSchema.validate(writersSchema.schemas[i].type, datum)) {
                schemaIndex = i;
                break;
            }
        }

        if (schemaIndex < 0) {
            throw new AvroErrors.IOError("No schema found for data %j", datum);
        } else {
            encoder.writeLong(schemaIndex);
            this.writeData(writersSchema.schemas[schemaIndex], datum, encoder);
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
