var _ = require('underscore');
var should = require('should');
require('buffertools');

var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var IO = require(libpath + "/../lib/io");
var DataFile = require(libpath + "/../lib/datafile");

describe('IO', function(){
    describe('BinaryEncoder()', function(){
        var encoder, writer;
        beforeEach(function(){
            writer = DataFile.Block();
            encoder = IO.BinaryEncoder(writer);
        })
        afterEach(function(){
            encoder = null;
        })
        it('should throw an error if it is not passed an object to write to', function(){
            (function() {
                var invalidEncoder = IO.BinaryEncoder();
            }).should.throwError();
        });
        it('should throw an error if the object passed in does not implement the writeBytes method', function() {
            (function() {
                var dummyBlock = { writeBytes: 0 };
                var invalidEncoder = IO.BinaryEncoder(dummyBlock);
            }).should.throwError();
        });
        describe('writeByte()', function(){
            it('should add a single octet to the buffer', function() {
                encoder.writeByte(50);
                writer.toBuffer()[0].should.equal(50);
                // Test high bit
                encoder.writeByte(250);
                writer.toBuffer()[1].should.equal(250);
            });
        });
        describe('writeNull()', function(){
            it('should not add anything to the buffer', function(){
                encoder.writeNull();
                writer.length.should.equal(0);
            });
        });
        describe('writeBoolean()', function(){
            it('should add 1 or 0 to the buffer', function(){
                encoder.writeBoolean(true);
                writer.toBuffer()[0].should.equal(1);
                encoder.writeBoolean(false);
                writer.toBuffer()[1].should.equal(0);
            });
        });
        describe('writeLong()', function(){
            it('should encode a long using zig-zag encoding', function(){
                encoder.writeLong(4);
                writer.toBuffer()[0].should.equal(8);
                encoder.writeLong(138);
                writer.toBuffer()[1].should.equal(148);
                writer.toBuffer()[2].should.equal(2);
            });
        });
        describe('writeFloat()', function(){
            it('should encode a 32bit float in 4 bytes using java floatToIntBits method', function(){
                encoder.writeFloat(1.3278991);
                writer.toBuffer().equals(new Buffer([0x99, 0xf8, 0xa9, 0x3f])).should.be.true;
            });
        });
        describe('writeDouble()', function(){
            it('should encode a 64bit float in 8 bytes using java doubleToLongBits method', function() {
                encoder.writeDouble(8.98928196620122323);
                writer.toBuffer().equals(new Buffer([0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40])).should.be.true;
            });
        });
        describe('writeFixed()', function(){
            it('should add a series of bytes specified by the schema', function(){
                var testString = "123456789abcdef";
                encoder.writeFixed(testString);
                writer.toBuffer().toString().should.equal(testString);
                writer.toBuffer().length.should.equal(testString.length);
            })
        });
        describe('writeBytes()', function(){
            it('should be encoded as a long followed by that many bytes of data', function(){
                var testBytes = new Buffer([255, 1, 254, 2, 253, 3]);
                encoder.writeBytes(testBytes);
                writer.toBuffer()[0].should.equal(testBytes.length * 2);
                writer.toBuffer()[5].should.equal(253);
            })
        });
        describe('writeString()', function(){
            it('should be encoded as a long followed by that many bytes of UTF8 encoded character data', function(){
                // Test UTF8 characters as well as normal
                var testString = "\u00A9 all rights reserved";
                encoder.writeString(testString);
                writer.toBuffer().equals(new Buffer([0x2c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c, 0x20,
                     0x72, 0x69, 0x67, 0x68, 0x74, 0x73, 0x20, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76,
                     0x65, 0x64])).should.be.true;
            })
        });
    });
    describe('BinaryDecoder()', function(){
        var decoder, block;
        beforeEach(function(){
            block = DataFile.Block();
            decoder = IO.BinaryDecoder(block);
        })
        afterEach(function(){
            block = null;
            decoder = null;
        })
        describe('readNull()', function(){
            it('should decode and return a null', function(){
                should.not.exist(decoder.readNull());
            });
        });
        describe('readByte()', function(){
            it('should decode and return an octet from the current position of the buffer', function(){
                block.write(new Buffer([0x55]));
                decoder.readByte().should.equal(0x55);
            })
        })
        describe('readBoolean()', function(){
            it('should decode and return true or false', function(){
                block.write(new Buffer([0x01, 0x00]));
                decoder.readBoolean().should.be.true;
                decoder.readBoolean().should.be.false;
            })
        })
        describe('readLong()', function(){
            it('should decode and return a long', function(){
                block.write(new Buffer([0x94, 0x02]));
                decoder.readLong().should.equal(138);
            })
        })
        describe('readFloat()', function(){
            it('should decode and return a 32bit float', function(){
                block.write(new Buffer([0x99, 0xf8, 0xa9, 0x3f]));
                decoder.readFloat().toFixed(7).should.equal('1.3278991');
            })
        })
        describe('readDouble()', function(){
            it('should decode and return a 64bit float', function(){
                block.write(new Buffer([0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40]));
                decoder.readDouble().should.equal(8.98928196620122323);
            })
        })
        describe('readFixed()', function(){
            it('should decode and return a fixed number of bytes', function(){
                block.write(new Buffer([0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC]));
                decoder.readFixed(8).equals(new Buffer([0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC])).should.be.true;
            })
        })
        describe('readBytes()', function(){
            it('should decode and return a set of bytes', function(){
                block.write(new Buffer([0x08, 0x11, 0x22, 0x33, 0x44]));
                decoder.readBytes().equals(new Buffer([0x11, 0x22, 0x33, 0x44]));
            })
        })
        describe('readString()', function(){
            it('should decode and return a string', function(){
                block.write(new Buffer([0x2c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c, 0x20,
                     0x72, 0x69, 0x67, 0x68, 0x74, 0x73, 0x20, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76,
                     0x65, 0x64]));
                decoder.readString().should.equal("\u00A9 all rights reserved");
            })
        })
        describe('skipNull()', function(){
            it('should be a no op since nulls are encoded a nothing', function(){
                block.write(new Buffer([1]));
                decoder.skipNull();
                block.remainingBytes().should.equal(1);
            })
        })
        describe('skipBoolean()', function(){
            it('should skip a reading by 1 byte', function(){
                block.write(new Buffer([1]));
                decoder.skipBoolean();
                block.remainingBytes().should.equal(0);
            });
        })
        describe('skipLong()', function(){
            it('should skip n bytes of a long encoded with zigzag encoding', function(){
                block.write(new Buffer([0x94, 0x02]));
                decoder.skipLong();
                block.remainingBytes().should.equal(0);
                block.write(new Buffer([0x02]));
                decoder.skipLong();
                block.remainingBytes().should.equal(0)
            })
        })
        describe('skipFloat()', function(){
            it('should skip 4 bytes of an encoded float', function(){
                block.write(new Buffer([0x40, 0x50, 0x60, 0x70]));
                decoder.skipFloat();
                block.remainingBytes().should.equal(0);
            })
        })
        describe('skipDouble()', function(){
            it('should skip 8 bytes of an encoded double', function(){
                block.write(new Buffer([0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0]));
                decoder.skipDouble();
                block.remainingBytes().should.equal(0);
            })
        })
        describe('skipBytes()', function(){
            it('should ', function(){
                block.write(new Buffer([0x04, 0x64, 0x40]))
                decoder.skipBytes();
                block.remainingBytes().should.equal(0);
            })
        })
        describe('skipString()', function(){
            it('should skip a long followed by that many bytes', function(){
                block.write(new Buffer([0x04, 0x4F, 0x4B]));
                decoder.skipString();
                block.remainingBytes().should.equal(0);
            });
            it('should skip a long followed by a UTF-8 encoded string', function(){
                block.write(new Buffer([0x0c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c]));
                decoder.skipString();
                block.remainingBytes().should.equal(0);
            });
        })
    })
    describe('DatumWriter()', function() {
        it('should be initiated and store a schema', function(){
            var schema = "long";
            var writer = IO.DatumWriter(schema);
            writer.writersSchema.should.equal(schema);
        })
        describe('writeEnum()', function(){
            it('should write an eneration encoded by its index', function(){
                var schema = {
                    "type": "enum",
                    "name": "phonetics",
                    "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
                };
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.writeEnum(schema, "Charlie", encoder);
                writer.writeEnum(schema, "Delta", encoder);
                block.toBuffer()[0].should.equal(4);
                block.toBuffer()[1].should.equal(6);
            });
        });
        describe('writeArray()', function(){
            it('should encode an array as a series of blocks, each block consists of a long count value, followed by that many array items, a block with count zero indicates the end of the array', function(){
                var schema = {
                    "type": "array",
                    "items": "long",
                };
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var testArray = [10, 20, 30, 40, 50];
                writer.writeArray(schema, testArray, encoder);
                block.toBuffer().equals(new Buffer([testArray.length * 2, 20, 40, 60, 80, 100, 0])).should.be.true;
            })
        });
        describe('writeMap()', function(){
            it('should write a map encoded as a series of blocks, each block consists of a long count, followed by that many key/value pairs, a block count of 0 indicates the end of the map', function(){
                var schema = {
                    "name": "headers",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                };
                var data = {
                    "user-agent": "firefox",
                    "remote-ip": "10.0.0.0",
                    "content-type": "applicaiton/json"
                }
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.writeMap(schema.type, data, encoder);
                var i = 0;
                block.toBuffer()[i++].should.equal(_.size(data) * 2); // zig-zag encoding
                _.each(data, function(value, key) {
                    block.toBuffer()[i++].should.equal(key.length * 2); // zig-zag encoding
                    block.toBuffer().slice(i,i + key.length).toString().should.equal(key);
                    i += key.length;
                    block.toBuffer()[i++].should.equal(value.length * 2); // zig-zag encoding
                    block.toBuffer().slice(i,i + value.length).toString().should.equal(value);
                    i += value.length;
                })
            });
        });
        describe('writeUnion()', function(){
            it('should encode a union by first writing a long value indicating the zero-based position within the union of the schema of its value, followed by the encoded value according to that schema', function(){
                var schema = [ "string", "int" ];
                var data = "testing a union";
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.writeUnion(schema, data, encoder);
                block.toBuffer().length.should.equal(data.length + 2);
                block.toBuffer()[0].should.equal(0);
                block.toBuffer()[1].should.equal(data.length * 2);
                block.toBuffer().slice(2).toString().should.equal(data);   
                block.flush();
                writer.writeUnion(schema, 44, encoder);
                block.toBuffer().length.should.equal(2);
                block.toBuffer()[0].should.equal(2);
                block.toBuffer()[1].should.equal(44 * 2);
            });
        });
        describe('writeRecord()', function(){
            it('should encode a record by encoding the values of its fields in the order that they are declared', function(){
                var schema = {
                    "name": "user",
                    "type": "record",
                    "fields": [
                        {"name":"firstName","type": "string"},
                        {"name":"lastName","type": "string"},
                        {"name":"age","type": "int"}
                    ]
                };
                var data = {
                    "firstName": "bob",
                    "lastName": "the_builder",
                    "age": 40
                }
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.writeRecord(schema, data, encoder);
                block.toBuffer()[0].should.equal(data.firstName.length * 2); // zig-zag encoding
                block.toBuffer().slice(1,4).toString().should.equal(data.firstName);
                block.toBuffer()[4].should.equal(data.lastName.length * 2); // zig-zag encoding
                block.toBuffer().slice(5,16).toString().should.equal(data.lastName);
                block.toBuffer()[16].should.equal(data.age * 2);
            })
        });
        describe('write()', function(){
            it('should encode an int/long with zig-zag encoding', function() {
                var schema = {
                    "type": "int"
                };
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.write(-64, encoder);
                block.toBuffer()[0].should.equal(127);
            });
            it('should encode a string as a long of its length, followed by the utf8 encoded string', function(){
                var schema = {
                    "type": "string"
                };
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.write("testing", encoder);
                block.toBuffer().toString().should.equal("\u000etesting");
            });
            it('should encode a record as the values of its fields in the order of declaration', function(){
                var schema = {
                    "type" : "record",
                    "name" : "IntStringRecord",
                    "fields" : [ { "name" : "intField", "type" : "int" },
                                 { "name" : "stringField", "type" : "string" }]
                };
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var record = {
                    intField: 1,
                    stringField: "abc"
                };
                writer.write(record, encoder);
                block.toBuffer().toString().should.equal("\u0002\u0006abc");
            });
            it('should encode a union as a long of the zero-based schema position, followed by the value according to the schema at that position', function(){
                var schema = [
                    "int",
                    "string",
                    "null"
                ];
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var record = {
                    "string": "test"
                }
                writer.write(record, encoder);
                block.toBuffer().toString().should.equal("\u0002\u0008test");
                block.flush();
                var record = {
                    "null": null
                };
                write.writer(record, encoder);
                writer.buffer()[0].should.equal(6);
            });
        });
    });
    describe('DatumReader()', function(){
        describe('read()', function(){
            it('should ', function(){
                should.exist(null);
            })
        })
        describe('readData()', function(){
            it('should ', function(){
                should.exist(null);
            })
        })
        describe('readEnum()', function(){
            it('should decode and return an enumerated type', function(){
                var schema = {
                    "type": "enum",
                    "name": "phonetics",
                    "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
                };
                var block = DataFile.Block();
                var reader = IO.DatumReader(schema);
                var decoder = IO.BinaryDecoder(block);
                block.write(new Buffer([0x06]));
                reader.readEnum(schema, schema, decoder).should.equal("Delta");
            })
        })
        describe('readArray()', function(){
            it('should decode and return an array', function(){
                var schema = {
                    "type": "array",
                    "items": "string"
                }
                var data = ["apples", "banannas", "oranges", "pears", "grapes"];
                var block = DataFile.Block();
                var reader = IO.DatumReader(schema);
                var decoder = IO.BinaryDecoder(block);
                block.write(new Buffer([0x0a, 0x0c, 0x61, 0x70, 0x70, 0x6c, 0x65, 0x73, 0x10, 0x62, 0x61, 
                                        0x6e, 0x61, 0x6e, 0x6e, 0x61, 0x73, 0x0e, 0x6f, 0x72, 0x61, 0x6e, 
                                        0x67, 0x65, 0x73, 0x0a, 0x70, 0x65, 0x61, 0x72, 0x73, 0x0c, 0x67, 
                                        0x72, 0x61, 0x70, 0x65, 0x73, 0x00]));
                reader.readArray(schema, schema, decoder).should.eql(data);
            })
        })
        describe('readMap()', function(){
            it('should decode a map and return a json object containing the data', function(){
                var schema = {
                    "name": "headers",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                };
                var data = [ 6, 20, 117, 115, 101, 114, 45, 97, 103, 101, 110, 116, 14, 102, 105, 114, 101, 
                             102, 111, 120, 18, 114, 101, 109, 111, 116, 101, 45, 105, 112, 16, 49, 48, 46, 
                             48, 46, 48, 46, 48, 24, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 
                             101, 32, 97, 112, 112, 108, 105, 99, 97, 105, 116, 111, 110, 47, 106, 115, 111, 
                             110, 0];            
                var block = DataFile.Block();
                block.write(new Buffer(data));
                var reader = IO.DatumReader(schema);
                var decoder = IO.BinaryDecoder(block);
                var map = reader.readMap(schema.type, schema.type, decoder);
                map.should.have.property("user-agent", "firefox");
                map.should.have.property("remote-ip", "10.0.0.0");
                map.should.have.property("content-type", "applicaiton/json");
            });
        })
        describe('readUnion()', function(){
            it('should ', function(){
                should.exist(null);
            })
        })
        describe('readRecord()', function(){
            it('should decode a record and return a json object containing the data', function(){
                var schema = {
                    "name": "user",
                    "type": "record",
                    "fields": [
                        {"name":"firstName","type": "string"},
                        {"name":"lastName","type": "string"},
                        {"name":"age","type": "int"}
                    ]
                };
                var block = DataFile.Block();
                block.write(new Buffer([0x06, 0x62, 0x6f, 0x62, 0x16, 0x74, 0x68, 0x65, 0x5f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x65, 0x72, 0x50]));
                var reader = IO.DatumReader(schema);
                var decoder = IO.BinaryDecoder(block);
                var record = reader.readRecord(schema, schema, decoder);
                record.should.have.property("firstName", "bob");
                record.should.have.property("lastName", "the_builder");
                record.should.have.property("age", 40);
            })
        });
        describe('skipData()', function(){
            it('should skip skip a specified field type', function(){
/*                var schema = {
                    "type": "enum",
                    "name": "phonetics",
                    "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
                };
                var decoder = IO.BinaryDecoder();
                decoder.setBuffer(new Buffer([0x06]));
                var reader = IO.DatumReader(schema, schema, decoder);
                reader.skipData(schema, null, decoder).should.equal("Delta");*/
                should.exist(null);
            })
        })
    })
})
