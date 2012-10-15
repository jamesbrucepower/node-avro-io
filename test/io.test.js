var assert = require("assert");
var should = require('should');
var _ = require('underscore');

var IO = require(__dirname + "/../lib/io");
var validator = require(__dirname + "/../lib/validator").Validator;
require('buffertools');

describe('IO', function(){
    describe('BinaryEncoder()', function(){
        describe('writeByte()', function(){
            it('should add a single octet to the buffer', function() {
                var encoder = IO.BinaryEncoder();
                encoder.writeByte(50);
                encoder.buffer()[0].should.equal(50);
                // Test high bit
                encoder.writeByte(250);
                encoder.buffer()[1].should.equal(250);
            });
        });
        describe('writeNull()', function(){
            it('should not add anything to the buffer', function(){
                var encoder = IO.BinaryEncoder();
                encoder.writeNull();
                encoder.buffer().length.should.equal(0);
            });
        });
        describe('writeBoolean()', function(){
            it('should add 1 or 0 to the buffer', function(){
                var encoder = IO.BinaryEncoder();
                encoder.writeBoolean(true);
                encoder.buffer()[0].should.equal(1);
                encoder.writeBoolean(false);
                encoder.buffer()[1].should.equal(0);
            });
        });
        describe('writeLong()', function(){
            it('should encode a long using zig-zag encoding', function(){
                var encoder = IO.BinaryEncoder();
                encoder.writeLong(4);
                encoder.buffer()[0].should.equal(8);
                encoder.writeLong(138);
                encoder.buffer()[1].should.equal(148);
                encoder.buffer()[2].should.equal(2);
            });
        });
        describe('writeFloat()', function(){
            it('should encode a 32bit float in 4 bytes using java floatToIntBits method', function(){
                var encoder = IO.BinaryEncoder();
                encoder.writeFloat(1.3278991);
                encoder.buffer().equals(new Buffer([0x99, 0xf8, 0xa9, 0x3f])).should.be.true;
            });
        });
        describe('writeDouble()', function(){
            it('should encode a 64bit float in 8 bytes using java doubleToLongBits method', function() {
                var encoder = IO.BinaryEncoder();
                encoder.writeDouble(8.98928196620122323);
                encoder.buffer().equals(new Buffer([0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40])).should.be.true;
            });
        });
        describe('writeFixed()', function(){
            it('should add a series of bytes specified by the schema', function(){
                var encoder = IO.BinaryEncoder();
                var testString = "123456789abcdef";
                encoder.writeFixed(testString);  
                encoder.buffer().toString().should.equal(testString);
                encoder.buffer().length.should.equal(testString.length);
            })
        });
        describe('writeBytes()', function(){
            it('should be encoded as a long followed by that many bytes of data', function(){
                var encoder = IO.BinaryEncoder();
                var testBytes = new Buffer([255, 1, 254, 2, 253, 3]);
                encoder.writeBytes(testBytes);  
                encoder.buffer()[0].should.equal(testBytes.length * 2);
                encoder.buffer()[5].should.equal(253);              
            })
        });
        describe('writeString()', function(){
            it('should be encoded as a long followed by that many bytes of UTF8 encoded character data', function(){
                var encoder = IO.BinaryEncoder();
                // Test UTF8 characters as well as normal
                var testString = "\u00A9 all rights reserved";
                encoder.writeString(testString);
                encoder.buffer().equals(new Buffer([0x2c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c, 0x20,
                     0x72, 0x69, 0x67, 0x68, 0x74, 0x73, 0x20, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 
                     0x65, 0x64])).should.be.true;
            })
        });
    });
    describe('BinaryDecoder()', function(){
        describe('readNull()', function(){
            it('should ', function(){
              
            });
        });
        describe('readByte()', function(){
            it('should ', function(){
              
            })
        })
        describe('readBoolean()', function(){
            it('should ', function(){
              
            })
        })
        describe('readLong()', function(){
            it('should ', function(){
              
            })
        })
        describe('readFloat()', function(){
            it('should ', function(){
              
            })
        })
        describe('readDouble()', function(){
            it('should ', function(){
              
            })
        })
        describe('readFixed()', function(){
            it('should ', function(){
              
            })
        })
        describe('readBytes()', function(){
            it('should ', function(){
              
            })
        })
        describe('readString()', function(){
            it('should ', function(){
              
            })
        })
        describe('readEnum()', function(){
            it('should ', function(){
              
            })
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
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                writer.writeEnum(schema, "Charlie", encoder);
                writer.writeEnum(schema, "Delta", encoder);
                encoder.buffer()[0].should.equal(4);
                encoder.buffer()[1].should.equal(6);
            });
        });
        describe('writeArray()', function(){
            it('should encode an array as a series of blocks, each block consists of a long count value, followed by that many array items, a block with count zero indicates the end of the array', function(){
                var schema = {
                    "type": "array",
                    "items": "long",
                };
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                var testArray = [10, 20, 30, 40, 50];
                writer.writeArray(schema, testArray, encoder);
                encoder.buffer().equals(new Buffer([testArray.length * 2, 20, 40, 60, 80, 100, 0])).should.be.true;
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
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                writer.writeMap(schema.type, data, encoder);
                var i = 0;
                encoder.buffer()[i++].should.equal(_.size(data) * 2); // zig-zag encoding
                _.each(data, function(value, key) {
                    encoder.buffer()[i++].should.equal(key.length * 2); // zig-zag encoding
                    encoder.buffer().slice(i,i + key.length).toString().should.equal(key);
                    i += key.length;
                    encoder.buffer()[i++].should.equal(value.length * 2); // zig-zag encoding
                    encoder.buffer().slice(i,i + value.length).toString().should.equal(value);
                    i += value.length;    
                })
            });
        });
        describe('writeUnion()', function(){
            it('should encode a union by first writing a long value indicating the zero-based position within the union of the schema of its value, followed by the encoded value according to that schema', function(){
                 
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
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                writer.writeRecord(schema, data, encoder);
                encoder.buffer()[0].should.equal(data.firstName.length * 2); // zig-zag encoding
                encoder.buffer().slice(1,4).toString().should.equal(data.firstName);
                encoder.buffer()[4].should.equal(data.lastName.length * 2); // zig-zag encoding
                encoder.buffer().slice(5,16).toString().should.equal(data.lastName);
                encoder.buffer()[16].should.equal(data.age * 2);
            })
        });
        describe('write()', function(){
            it('should encode an int/long with zig-zag encoding', function() {
                var schema = {
                    "type": "int"
                };
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                writer.write(-64, encoder);
                encoder.buffer()[0].should.equal(127);          
            });
            it('should encode a string as a long of its length, followed by the utf8 encoded string', function(){
                var schema = {
                    "type": "string"
                };
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                writer.write("testing", encoder);
                encoder.buffer().toString().should.equal("\u000etesting");          
            });
            it('should encode a record as the values of its fields in the order of declaration', function(){
                var schema = {
                    "type" : "record", 
                    "name" : "IntStringRecord", 
                    "fields" : [ { "name" : "intField", "type" : "int" }, 
                                 { "name" : "stringField", "type" : "string" }]
                };
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                var record = {
                    intField: 1,
                    stringField: "abc"
                };
                validator.validate(schema, record);
                writer.write(record, encoder);
                encoder.buffer().toString().should.equal("\u0002\u0006abc");
            });
            it('should encode a union as a long of the zero-based schema position, followed by the value according to the schema at that position', function(){
                var schema = [
                    "int", 
                    "string",
                    "null" 
                ];
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder();
                var record = {
                    "string": "test"
                }
                writer.write(record, encoder);
                encoder.buffer().toString().should.equal("\u0002\u0008test");
                encoder.flush();
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
              
            })
        })
        describe('readData()', function(){
            it('should ', function(){
              
            })
        })
        describe('readEnum()', function(){
            it('should ', function(){
              
            })
        })
        describe('readArray()', function(){
            it('should ', function(){
              
            })
        })
        describe('readMap()', function(){
            it('should ', function(){
              
            })
        })
        describe('readUnion()', function(){
            it('should ', function(){
              
            })
        })
        describe('readRecord()', function(){
            it('should ', function(){
              
            })
        })
    })
})