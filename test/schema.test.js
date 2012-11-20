var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';

var _ = require('underscore');
var should = require('should');

var Schema = require(libpath + 'schema');

describe('Schema()', function(){
    it('should create a new Schema object given arguments', function(){
        var schema = Schema.Schema("string");
        schema.should.be.an.instanceof(Schema.PrimitiveSchema);
        schema.should.be.an.instanceof(Schema.Schema); // its baseclass
        schema.type.should.equal("string");
    });
    describe('parse()', function(){
        it('should throw an exception if no arguments are provided', function(){
            (function() {
                var schema = Schema.Schema();
                schema.parse();
            }).should.throwError();
        });      
        it('should return a PrimitiveSchema if any of the primitive types are passed as schema arguments', function(){
            var primitives = ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'];
            _.each(primitives, function(type) {
                var schema = Schema.Schema(type);
                schema.should.be.an.instanceof(Schema.PrimitiveSchema);
                schema.type.should.equal(type);                
            });
        });
        it('should return a PrimitiveSchema if any one of the primitive types are passed as a type', function(){
            var primitives = ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'];
            _.each(primitives, function(type) {
                var schema = Schema.Schema({ "type": type });
                schema.should.be.an.instanceof(Schema.PrimitiveSchema);
                schema.type.should.equal(type);                
            });          
        })
        it('should return a UnionSchema if an array is passwd as a type', function(){
            var schema = Schema.Schema([ "string", "int", "null"]);
            schema.should.be.an.instanceof(Schema.UnionSchema);
        });
        it('should return a RecordSchema if an object is passed with a type "record"', function(){
            var schema = Schema.Schema({ 
                name: "myrecord", 
                type: "record", 
                fields: []
            });
            schema.should.be.an.instanceof(Schema.RecordSchema);
        });
    })
    
})