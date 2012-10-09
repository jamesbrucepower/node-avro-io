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
    });
})