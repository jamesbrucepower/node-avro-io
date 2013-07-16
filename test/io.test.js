var _ = require('underscore');
var should = require('should');
require('buffertools');

var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';
var IO = require(libpath + 'io');
var DataFile = require(libpath + 'datafile');
var Avro = require(libpath + 'schema');

function randomString(length) {
    return new Buffer(new DataFile.Writer()._generateSyncMarker(32)).toString('base64');
}

describe('IO', function(){
    describe('BinaryEncoder()', function(){
        var encoder, block;
        beforeEach(function(){
            block = DataFile.Block();
            encoder = IO.BinaryEncoder(block);
        })
        afterEach(function(){
            encoder = null;
        })
        it('should throw an error if it is not passed an object to write to', function(){
            (function() {
                var invalidEncoder = IO.BinaryEncoder();
            }).should.throwError();
        });
        it('should throw an error if the object passed in does not implement the write() method', function() {
            (function() {
                var dummyBlock = { write: 0 };
                var invalidEncoder = IO.BinaryEncoder(dummyBlock);
            }).should.throwError();
        });
        describe('writeByte()', function(){
            it('should add a single octet to the buffer', function() {
                encoder.writeByte(50);
                block.toBuffer()[0].should.equal(50);
                // Test high bit
                encoder.writeByte(250);
                block.toBuffer()[1].should.equal(250);
            });
        });
        describe('writeNull()', function(){
            it('should not add anything to the buffer', function(){
                encoder.writeNull();
                block.length.should.equal(0);
            });
        });
        describe('writeBoolean()', function(){
            it('should add 1 or 0 to the buffer', function(){
                encoder.writeBoolean(true);
                block.toBuffer()[0].should.equal(1);
                encoder.writeBoolean(false);
                block.toBuffer()[1].should.equal(0);
            });
        });
        describe('writeLong()', function(){
            it('should encode a long using zig-zag encoding', function(){
                encoder.writeLong(4);
                block.toBuffer()[0].should.equal(8);
                encoder.writeLong(138);
                block.toBuffer()[1].should.equal(148);
                block.toBuffer()[2].should.equal(2);
            });
        });
        describe('writeFloat()', function(){
            it('should encode a 32bit float in 4 bytes using java floatToIntBits method', function(){
                encoder.writeFloat(1.3278991);
                block.toBuffer().equals(new Buffer([0x99, 0xf8, 0xa9, 0x3f])).should.be.true;
            });
        });
        describe('writeDouble()', function(){
            it('should encode a 64bit float in 8 bytes using java doubleToLongBits method', function() {
                encoder.writeDouble(8.98928196620122323);
                block.toBuffer().equals(new Buffer([0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40])).should.be.true;
            });
        });
        describe('writeBytes()', function(){
            it('should be encoded as a long followed by that many bytes of data', function(){
                var testBytes = new Buffer([255, 1, 254, 2, 253, 3]);
                encoder.writeBytes(testBytes);
                block.toBuffer()[0].should.equal(testBytes.length * 2);
                block.toBuffer()[5].should.equal(253);
            });
            it('should throw an error if a buffer or array is not provided', function(){
                (function() {
                    encoder.writeBytes(4);
                }).should.throwError();
            })
        });
        describe('writeString()', function(){
            it('should be encoded as a long followed by that many bytes of UTF8 encoded character data', function(){
                // Test UTF8 characters as well as normal
                var testString = "\u00A9 all rights reserved";
                encoder.writeString(testString);
                block.toBuffer().equals(new Buffer([0x2c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c, 0x20,
                     0x72, 0x69, 0x67, 0x68, 0x74, 0x73, 0x20, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76,
                     0x65, 0x64])).should.be.true;
            });
            it('should throw an error if is not passed a string', function(){
                (function() {
                    encoder.writeString(21);
                }).should.throwError();
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
        it('should throw an error if the constructor is not passed an input object', function(){
            (function() {
                var invalidDecoder = IO.BinaryDecoder();
            }).should.throwError();
        });
        it('should throw an error if the constructor is not passed an input object that implements the read method', function(){
            (function() {
                var dummyBlock = { read: false };
                var invalidDecoder = IO.BinaryDecoder(dummyBlock);
            }).should.throwError();
        });
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
                block.remainingBytes.should.equal(1);
            })
        })
        describe('skipBoolean()', function(){
            it('should skip a reading by 1 byte', function(){
                block.write(new Buffer([1]));
                decoder.skipBoolean();
                block.remainingBytes.should.equal(0);
            });
        })
        describe('skipLong()', function(){
            it('should skip n bytes of a long encoded with zigzag encoding', function(){
                block.write(new Buffer([0x94, 0x02]));
                decoder.skipLong();
                block.remainingBytes.should.equal(0);
                block.write(new Buffer([0x02]));
                decoder.skipLong();
                block.remainingBytes.should.equal(0)
            })
        })
        describe('skipFloat()', function(){
            it('should skip 4 bytes of an encoded float', function(){
                block.write(new Buffer([0x40, 0x50, 0x60, 0x70]));
                decoder.skipFloat();
                block.remainingBytes.should.equal(0);
            })
        })
        describe('skipDouble()', function(){
            it('should skip 8 bytes of an encoded double', function(){
                block.write(new Buffer([0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0]));
                decoder.skipDouble();
                block.remainingBytes.should.equal(0);
            })
        })
        describe('skipBytes()', function(){
            it('should ', function(){
                block.write(new Buffer([0x04, 0x64, 0x40]))
                decoder.skipBytes();
                block.remainingBytes.should.equal(0);
            })
        })
        describe('skipString()', function(){
            it('should skip a long followed by that many bytes', function(){
                block.write(new Buffer([0x04, 0x4F, 0x4B]));
                decoder.skipString();
                block.remainingBytes.should.equal(0);
            });
            it('should skip a long followed by a UTF-8 encoded string', function(){
                block.write(new Buffer([0x0c, 0xc2, 0xa9, 0x20, 0x61, 0x6c, 0x6c]));
                decoder.skipString();
                block.remainingBytes.should.equal(0);
            });
        })
    })
    describe('DatumWriter()', function() {
        it('should be initiated and store a schema', function(){
            var schema = Avro.Schema("long");
            var writer = IO.DatumWriter(schema);
            writer.writersSchema.should.equal(schema);
        })
        describe('writeFixed()', function(){
            it('should add a series of bytes specified by the schema', function(){
                var schema = Avro.Schema({
                    "type": "fixed",
                    "name": "telephone",
                    "size": 10
                });
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var testString = "1234567890";
                writer.writeFixed(schema, testString, encoder);
                block.toBuffer().toString().should.equal(testString);
                block.toBuffer().length.should.equal(testString.length);
            })
        });
        describe('writeEnum()', function(){
            it('should write an eneration encoded by its index', function(){
                var schema = Avro.Schema({
                    "type": "enum",
                    "name": "phonetics",
                    "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
                });
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
                var schema = Avro.Schema({
                    "type": "array",
                    "items": "long",
                });
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
                var schema = Avro.Schema({
                    "name": "headers",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                });
                var data = {
                    "user-agent": "firefox",
                    "remote-ip": "10.0.0.0",
                    "content-type": "applicaiton/json"
                }
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.writeMap(schema, data, encoder);
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
                var schema = Avro.Schema([ "string", "int" ]);
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
                var schema = Avro.Schema({
                    "name": "user",
                    "type": "record",
                    "fields": [
                        {"name":"firstName","type": "string"},
                        {"name":"lastName","type": "string"},
                        {"name":"age","type": "int"}
                    ]
                });
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
                var schema = Avro.Schema({
                    "type": "int"
                });
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.write(-64, encoder);
                block.toBuffer()[0].should.equal(127);
            });
            it('should encode a string as a long of its length, followed by the utf8 encoded string', function(){
                var schema = Avro.Schema({
                    "type": "string"
                });
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                writer.write("testing", encoder);
                block.toBuffer().toString().should.equal("\u000etesting");
            });
            it('should encode a record as the values of its fields in the order of declaration', function(){
                var schema = Avro.Schema({
                    "type" : "record",
                    "name" : "IntStringRecord",
                    "fields" : [ { "name" : "intField", "type" : "int" },
                                 { "name" : "stringField", "type" : "string" }]
                });
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
                var schema = Avro.Schema([
                    "int",
                    "string",
                    "null"
                ]);
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var record = "test";
                writer.write(record, encoder);
                block.toBuffer().toString().should.equal("\u0002\u0008test");
                block.flush();
                var record = null;
                writer.write(record, encoder);
                block.toBuffer()[0].should.equal(4);
            });
            it('should encode a nested schema', function() {
                var schema = Avro.Schema({
                    "fields": [
                        {
                            "name": "host", 
                            "type": "string"
                        }, 
                        {
                            "name": "time", 
                            "type": "string"
                        }, 
                        {
                            "name": "elapsedTime", 
                            "type": "long"
                        }, 
                        {
                            "name": "request", 
                            "type": {
                                "name": "Request", 
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "headers", 
                                        "type": {
                                            "type": "map", 
                                            "values": "string"
                                        }
                                    }, 
                                    {
                                        "name": "method", 
                                        "type": "string"
                                    }, 
                                    {
                                        "name": "path", 
                                        "type": "string"
                                    }, 
                                    {
                                        "name": "queryString", 
                                        "type": [
                                            "string", 
                                            "null"
                                        ]
                                    }, 
                                    {
                                        "name": "body", 
                                        "type": {
                                            "type": "map", 
                                            "values": "string"
                                        }
                                    }
                                ]
                            }
                        }, 
                        {
                            "name": "exception", 
                            "type": [
                                {
                                    "fields": [
                                        {
                                            "name": "class", 
                                            "type": "string"
                                        }, 
                                        {
                                            "name": "message", 
                                            "type": "string"
                                        }, 
                                        {
                                            "name": "stackTrace", 
                                            "type": [
                                                "null", 
                                                "string"
                                            ]
                                        }
                                    ], 
                                    "name": "AppException", 
                                    "type": "record"
                                }, 
                                "null"
                            ]
                        }
                    ], 
                    "name": "LogEvent", 
                    "namespace": "e.d.c.b.a", 
                    "type": "record"
                });
                var block = DataFile.Block();
                var writer = IO.DatumWriter(schema);
                var encoder = IO.BinaryEncoder(block);
                var log = {
                    host: "testhostA",
                    time: "1970-01-01T00:00Z",
                    elapsedTime: 123456789, 
                    request: {
                        headers: {
                            "user-agent": "firefox",
                            "remote-ip": "0.0.0.0"
                        },
                        method: "GET",
                        path: "/basepath/object",
                        queryString: "param1=test1&param2=test2",
                        body: {}
                    },
                    exception: {
                        "AppException": {
                            "class": "org.apache.avro",
                            message: "An error occurred",
                            stackTrace: "failed at line 1"
                        }
                    }
                }
                writer.write(log, encoder);
                block.toBuffer().equals(new Buffer([0x12, 0x74, 0x65, 0x73, 0x74,
                                                    0x68, 0x6f, 0x73, 0x74, 0x41, 0x22, 0x31, 0x39,
                                                    0x37, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x31,
                                                    0x54, 0x30, 0x30, 0x3a, 0x30, 0x30, 0x5a, 0xaa,
                                                    0xb4, 0xde, 0x75, 0x04, 0x14, 0x75, 0x73, 0x65,
                                                    0x72, 0x2d, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x0e,
                                                    0x66, 0x69, 0x72, 0x65, 0x66, 0x6f, 0x78, 0x12,
                                                    0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2d, 0x69,
                                                    0x70, 0x0e, 0x30, 0x2e, 0x30, 0x2e, 0x30, 0x2e,
                                                    0x30, 0x00, 0x06, 0x47, 0x45, 0x54, 0x20, 0x2f,
                                                    0x62, 0x61, 0x73, 0x65, 0x70, 0x61, 0x74, 0x68,
                                                    0x2f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x00,
                                                    0x32, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x31, 0x3d,
                                                    0x74, 0x65, 0x73, 0x74, 0x31, 0x26, 0x70, 0x61,
                                                    0x72, 0x61, 0x6d, 0x32, 0x3d, 0x74, 0x65, 0x73,
                                                    0x74, 0x32, 0x00, 0x00, 0x1e, 0x6f, 0x72, 0x67,
                                                    0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e,
                                                    0x61, 0x76, 0x72, 0x6f, 0x22, 0x41, 0x6e, 0x20,
                                                    0x65, 0x72, 0x72, 0x6f, 0x72, 0x20, 0x6f, 0x63,
                                                    0x63, 0x75, 0x72, 0x72, 0x65, 0x64, 0x02, 0x20,
                                                    0x66, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x20, 0x61,
                                                    0x74, 0x20, 0x6c, 0x69, 0x6e, 0x65, 0x20, 0x31])).should.be.true;
            })
        });
    });
    describe('DatumReader()', function(){
        var block, decoder;
        beforeEach(function(){
            block = DataFile.Block();
            decoder = IO.BinaryDecoder(block);
        });
        describe('read()', function(){
            it('should set the readersSchema to the writersSchema if readersSchema is null', function(){
                var schema = Avro.Schema("int");
                var reader = IO.DatumReader(schema, null);
                block.write(new Buffer([0x06]));
                var result = reader.read(decoder);
                result.should.equal(3);
                reader.writersSchema.should.equal(reader.readersSchema);
            });
        });
        describe('readData()', function(){
            var schema = Avro.Schema({
                "name": "testRecord",
                "type": "record",
                "fields": [
                    {"name":"testNull","type": "null"},
                    {"name":"testBoolean","type": "boolean"},
                    {"name":"testString","type": "string"},
                    {"name":"testInt","type": "int"},
                    {"name":"testLong","type": "long"},
                    {"name":"testFloat","type": "float"},
                    {"name":"testDouble","type": "double"},
                    {"name":"testBytes","type": "bytes"},
                    {"name":"testFixed","type": "fixed", "size": 5},
                    {"name":"testEnum","type": "enum", "symbols": ["Alpha", "Bravo", "Charlie", "Delta"]},
                    {"name":"testArray","type": "array", "items": "long"},                    
                    {"name":"testMap","type": { "type":"map", "values": "int"}},                    
                    {"name":"testUnion","type":["string", "int", "null"]}
                ]
            });
            schema.should.be.an.instanceof(Avro.RecordSchema);
            var reader = IO.DatumReader(schema);
            var block = DataFile.Block();
            var decoder = IO.BinaryDecoder(block);
            block.write(new Buffer([/*purposely blank*/
                                    0x01, 
                                    0x08, 0x74, 0x65, 0x73, 0x74,
                                    0x08, 
                                    0x94, 0x02,
                                    0x99, 0xf8, 0xa9, 0x3f,
                                    0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40,
                                    0x0c, 0xF4, 0x44, 0x45, 0x7f, 0x28, 0x6C,
                                    0x19, 0x69, 0x29, 0x3f, 0xff,
                                    0x04, 
                                    0x08, 0x14, 0x69, 0x10, 0xF1, 0x01, 0x00,
                                    0x06, 0x06, 0x6f, 0x6e, 0x65, 0x20, 0x06, 0x74, 0x77, 0x6f, 0x10, 0x0a, 0x74, 0x68, 0x72, 0x65, 0x65, 0x40, 0x00,
                                    0x04]));
            it('should read and decode a null', function(){
                var result = reader.readData(schema.fieldsHash["testNull"].type, null, decoder);
                should.not.exist(result);
                block.offset.should.equal(0);                
            });
            it('should read and decode a boolean', function(){
                var result = reader.readData(schema.fieldsHash["testBoolean"].type, null, decoder);
                result.should.equal(true);
            });
            it('should read and decode a string', function(){
                var result = reader.readData(schema.fieldsHash["testString"].type, null, decoder);
                result.should.equal("test");
            });
            it('should read and decode an int', function(){
                var result = reader.readData(schema.fieldsHash["testInt"].type, null, decoder);
                result.should.equal(4);
            });
            it('should read and decode a long', function(){
                var result = reader.readData(schema.fieldsHash["testLong"].type, null, decoder);
                result.should.equal(138);
            });
            it('should read and decode a float', function(){
                var result = reader.readData(schema.fieldsHash["testFloat"].type, null, decoder);
                result.toFixed(7).should.equal('1.3278991')
            });
            it('should read and decode a double', function(){
                var result = reader.readData(schema.fieldsHash["testDouble"].type, null, decoder);
                result.should.equal(8.98928196620122323);
            });
            it('should read and decode bytes', function(){
                var result = reader.readData(schema.fieldsHash["testBytes"].type, null, decoder);
                result.equals(new Buffer([0xF4, 0x44, 0x45, 0x7f, 0x28, 0x6C])).should.be.true;
                result.length.should.equal(6);
            });
            it('should read and decode a fixed', function(){
                var result = reader.readData(schema.fieldsHash["testFixed"].type, null, decoder);
                result.equals(new Buffer([0x19, 0x69, 0x29, 0x3f, 0xff])).should.be.true;
                result.length.should.equal(5);
            });
            it('should read and decode an enum', function(){
                var result = reader.readData(schema.fieldsHash["testEnum"].type, null, decoder);
                result.should.equal("Charlie");
            });
            it('should read and decode an array', function(){
                var result = reader.readData(schema.fieldsHash["testArray"].type, null, decoder);
                result.should.eql([10, -53, 8, -121]);
                result.length.should.equal(4);
            });
            it('should read and decode a map', function(){
                var result = reader.readData(schema.fieldsHash["testMap"].type, null, decoder);
                result.should.have.property("one", 0x10);
                result.should.have.property("two", 8);
                result.should.have.property("three", 0x20);
                _.size(result).should.equal(3);
            });
            it('should read and decode a union', function(){
                var result = reader.readData(schema.fieldsHash["testUnion"].type, null, decoder);
                should.not.exist(result);
            });
            it('should read and decode a record', function(){
                block.rewind();
                var result = reader.readData(schema, null, decoder);
                result.should.have.property("testMap");
                var map = result.testMap;
                map.should.have.property("one", 0x10);
            });
            it('should throw an error if an unrecognized schema type is provided', function(){
                (function() {
                    reader.readData(Avro.schema({"type":"invalid"}), null, decoder);
                }).should.throwError();
            });
            it('should throw an error if the writersSchema provided is not a Schema object', function(){
                (function() {
                    reader.readData("invalid", null, decoder);
                }).should.throwError();              
            });
            it('should throw an error if the readersSchema provided is not a Schema object', function(){
                (function() {
                    reader.readData(Avro.schema({"type":"string"}), "invalid", decoder);
                }).should.throwError();              
            });
        })
        describe('readEnum()', function(){
            it('should decode and return an enumerated type', function(){
                var schema = Avro.Schema({
                    "type": "enum",
                    "name": "phonetics",
                    "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
                });
                var reader = IO.DatumReader(schema);
                block.write(new Buffer([0x06]));
                reader.readEnum(schema, schema, decoder).should.equal("Delta");
            })
        })
        describe('readArray()', function(){
            it('should decode and return an array', function(){
                var schema = Avro.Schema({
                    "type": "array",
                    "items": "string"
                });
                var data = ["apples", "banannas", "oranges", "pears", "grapes"];
                var reader = IO.DatumReader(schema);
                block.write(new Buffer([0x0a, 0x0c, 0x61, 0x70, 0x70, 0x6c, 0x65, 0x73, 0x10, 0x62, 0x61, 
                                        0x6e, 0x61, 0x6e, 0x6e, 0x61, 0x73, 0x0e, 0x6f, 0x72, 0x61, 0x6e, 
                                        0x67, 0x65, 0x73, 0x0a, 0x70, 0x65, 0x61, 0x72, 0x73, 0x0c, 0x67, 
                                        0x72, 0x61, 0x70, 0x65, 0x73, 0x00]));
                reader.readArray(schema, schema, decoder).should.eql(data);
            })
        })
        describe('readMap()', function(){
            it('should decode a map and return a json object containing the data', function(){
                var schema = Avro.Schema({
                    "name": "headers",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                });
                var data = [ 6, 20, 117, 115, 101, 114, 45, 97, 103, 101, 110, 116, 14, 102, 105, 114, 101, 
                             102, 111, 120, 18, 114, 101, 109, 111, 116, 101, 45, 105, 112, 16, 49, 48, 46, 
                             48, 46, 48, 46, 48, 24, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 
                             101, 32, 97, 112, 112, 108, 105, 99, 97, 105, 116, 111, 110, 47, 106, 115, 111, 
                             110, 0];            
                block.write(new Buffer(data));
                var reader = IO.DatumReader(schema);
                var map = reader.readMap(schema, schema, decoder);
                map.should.have.property("user-agent", "firefox");
                map.should.have.property("remote-ip", "10.0.0.0");
                map.should.have.property("content-type", "applicaiton/json");
            });
        })
        describe('readUnion()', function(){
            it('should decode a union by returning the object specified by the schema of the unions index', function(){
                var schema = Avro.Schema([
                    "int",
                    "string",
                    "null"
                ]);
                var reader = IO.DatumReader(schema);
                block.write(new Buffer([0x02, 0x08, 0x74, 0x65, 0x73, 0x74]));
                var result = reader.readUnion(schema, schema, decoder);
                (result === "test").should.be.true;
            })
        })
        describe('readRecord()', function(){
            it('should decode a record and return a json object containing the data', function(){
                var schema = Avro.Schema({
                    "name": "user",
                    "type": "record",
                    "fields": [
                        {"name":"firstName","type": "string"},
                        {"name":"lastName","type": "string"},
                        {"name":"age","type": "int"}
                    ]
                });
                block.write(new Buffer([0x06, 0x62, 0x6f, 0x62, 0x16, 0x74, 0x68, 0x65, 0x5f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x65, 0x72, 0x50]));
                var reader = IO.DatumReader(schema);
                var record = reader.readRecord(schema, schema, decoder);
                record.should.have.property("firstName", "bob");
                record.should.have.property("lastName", "the_builder");
                record.should.have.property("age", 40);
            })
        });
        describe('skipData()', function(){
            var schema = Avro.Schema({
                "name": "testRecord",
                "type": "record",
                "fields": [
                    {"name":"testNull","type": "null"},
                    {"name":"testBoolean","type": "boolean"},
                    {"name":"testString","type": "string"},
                    {"name":"testInt","type": "int"},
                    {"name":"testLong","type": "long"},
                    {"name":"testFloat","type": "float"},
                    {"name":"testDouble","type": "double"},
                    {"name":"testBytes","type": "bytes"},
                    {"name":"testFixed","type": "fixed", "size": 5},
                    {"name":"testEnum","type": "enum", "symbols": ["Alpha", "Bravo", "Charlie", "Delta"]},
                    {"name":"testArray","type": "array", "items": "long"},                    
                    {"name":"testMap","type": { "type":"map", "values": "int"}},                    
                    {"name":"testUnion","type":["string", "int", "null"]}
                ]
            });
            var reader = IO.DatumReader(schema);
            var block = DataFile.Block();
            var decoder = IO.BinaryDecoder(block);
            block.write(new Buffer([/*purposely blank*/
                                    0x01, 
                                    0x08, 0x74, 0x65, 0x73, 0x74,
                                    0x08, 
                                    0x94, 0x02,
                                    0x99, 0xf8, 0xa9, 0x3f,
                                    0xb3, 0xb6, 0x76, 0x2a, 0x83, 0xfa, 0x21, 0x40,
                                    0x0c, 0xF4, 0x44, 0x45, 0x7f, 0x28, 0x6C,
                                    0x19, 0x69, 0x29, 0x3f, 0xff,
                                    0x04, 
                                    0x08, 0x14, 0x69, 0x10, 0xF1, 0x01, 0x00,
                                    0x06, 0x06, 0x6f, 0x6e, 0x65, 0x20, 0x06, 0x74, 0x77, 0x6f, 0x10, 0x0a, 0x74, 0x68, 0x72, 0x65, 0x65, 0x40, 0x00,
                                    0x04]));
            it('should skip a null', function(){
                reader.skipData(schema.fieldsHash["testNull"].type, decoder);
                block.offset.should.equal(0);
            });
            it('should skip a boolean', function(){
                reader.skipData(schema.fieldsHash["testBoolean"].type, decoder);
                block.offset.should.equal(1);
            });
            it('should skip a string', function(){
                reader.skipData(schema.fieldsHash["testString"].type, decoder);
                block.offset.should.equal(6);
            });
            it('should skip an int', function(){
                reader.skipData(schema.fieldsHash["testInt"].type, decoder);
                block.offset.should.equal(7);
            });
            it('should skip a long', function(){
                reader.skipData(schema.fieldsHash["testLong"].type, decoder);
                block.offset.should.equal(9);
            });
            it('should skip a float', function(){
                reader.skipData(schema.fieldsHash["testFloat"].type, decoder);
                block.offset.should.equal(13);
            });
            it('should skip a double', function(){
                reader.skipData(schema.fieldsHash["testDouble"].type, decoder);
                block.offset.should.equal(21);
            });
            it('should skip bytes', function(){
                reader.skipData(schema.fieldsHash["testBytes"].type, decoder);
                block.offset.should.equal(28);
            });
            it('should skip a fixed', function(){
                reader.skipData(schema.fieldsHash["testFixed"].type, decoder);
                block.offset.should.equal(33);
            });
            it('should skip an enum', function(){
                reader.skipData(schema.fieldsHash["testEnum"].type, decoder);
                block.offset.should.equal(34);
            });
            it('should skip an array', function(){
                reader.skipData(schema.fieldsHash["testArray"].type, decoder);
                block.offset.should.equal(41);
            });
            it('should skip a map', function(){
                reader.skipData(schema.fieldsHash["testMap"].type, decoder);
                block.offset.should.equal(60);
            });
            it('should skip a union', function(){
                reader.skipData(schema.fieldsHash["testUnion"].type, decoder);
                block.offset.should.equal(61);
            });
            it('should skip a record', function(){
                block.rewind();
                reader.skipData(schema, decoder);
                block.offset.should.equal(61);
            });
        })
    })
})
