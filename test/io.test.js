var assert = require("assert");
var should = require('should');

var IO = require(__dirname + "/../io");
var validator = require(__dirname + "/../validator").Validator;

describe('IO', function(){
    describe('DatumWriter()', function() {
        it('should encode an int/long with zig-zag encoding', function() {
            var schema = {
                "type": "int"
            };
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            writer.write(-64, encoder);
            writer.buffer.should.equal("ÿ\u0000");          
        });
        it('should encode a string as a long of its length, followed by the utf8 encoded string', function(){
            var schema = {
                "type": "string"
            };
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            writer.write("testing", encoder);
            writer.buffer.should.equal("\u000etesting");          
        });
        it('should encode a record as the values of its fields in the order of declaration', function(){
            var schema = {
                "type" : "record", 
                "name" : "IntStringRecord", 
                "fields" : [ { "name" : "intField", "type" : "int" }, 
                             { "name" : "stringField", "type" : "string" }]
            };
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            var record = {
                intField: 1,
                stringField: "abc"
            };
            validator.validate(schema, record);
            writer.write(record, encoder);
            writer.buffer.should.equal("\u0002\u0006abc");
        });
        it('should encode a union as a long of the zero-based schema position, followed by the value according to the schema at that position', function(){
         /*   var schema = [
                "int", 
                "string",
                "null" 
            ];
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            var record = {
                "string": "test"
            }
            writer.write(record, encoder);
            writer.buffer.should.equal("\u0002\u0008test");
            writer.buffer = "";
            writer.idx = 0;
            var record = {
                "null": null
            };
            write.writer(record, encoder);
            writer.buffer.should.equal("\u0006");*/
        })
    });
})