var assert = require("assert");
var should = require('should');

var IO = require(__dirname + "/../io");
var validator = require(__dirname + "/../validator").Validator;

describe('IO', function(){
    describe('DatumWriter()', function() {
        it('should encode an int/long with zig-zag encoding', function() {
            var schema = {"type":"int"};
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            writer.write(-64, encoder);
            writer.buffer.should.equal("ÿ\u0000");          
        });
        it('should encode a string as a long of its length, followed by the utf8 encoded string', function(){
            var schema = {"type":"string"};
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            writer.write("testing", encoder);
            writer.buffer.should.equal("\u000etesting");          
        });
        it('should encode a record as the values of its fields in the order of declaration', function(){
            var schema = {"type" : "record", "name" : "ShippingServiceOption", "fields" : [ { "name" : "field1", "type" : "int" }, { "name" : "field2", "type" : "string" }]};
            var writer = IO.DatumWriter(schema);
            var encoder = IO.BinaryEncoder(writer);
            var record = {
                "field1": 1,
                "field2": "abc"
            };
            writer.write(record, encoder);
            console.log("%j", writer.buffer);
            writer.buffer.should.exist;
        })
    });
})