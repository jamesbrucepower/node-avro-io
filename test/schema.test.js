var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';

var _ = require('underscore');
var should = require('should');

var Avro = require(libpath + 'schema');

describe('Schema()', function(){
    it('should create a new Schema object given arguments', function(){
        var schema = Avro.Schema("string");
        schema.should.be.an.instanceof(Avro.PrimitiveSchema);
        schema.should.be.an.instanceof(Avro.Schema); // its baseclass
        schema.type.should.equal("string");
    });
    describe('parse()', function(){
        it('should throw an error if no arguments are provided', function(){
            (function() {
                var schema = Avro.Schema();
                schema.parse();
            }).should.throwError();
        });      
        it('should return a PrimitiveSchema if any of the primitive types are passed as schema arguments or as a type property', function(){
            var primitives = ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'];
            _.each(primitives, function(type) {
                var schema = Avro.Schema(type);
                schema.should.be.an.instanceof(Avro.PrimitiveSchema);
                schema.type.should.equal(type);  
                schema = Avro.Schema({ "type": type });
                schema.should.be.an.instanceof(Avro.PrimitiveSchema);
                schema.type.should.equal(type);                 
            });
        });
        it('should throw an error is an unrecognized primitive type is provided', function(){
            (function() {
                Avro.Schema("unrecognized");
            }).should.throwError();
            (function() {
                Avro.Schema({"type":"unrecognized"});
            }).should.throwError();
        })
        it('should return a UnionSchema if an array is passwd as a type', function(){
            var schema = Avro.Schema([ "string", "int", "null"]);
            schema.should.be.an.instanceof(Avro.UnionSchema);
            schema.type.should.equal("union");
        });
        it('should throw an error if an empty array of unions is passed', function(){
            (function() {
                var schema = Avro.Schema([]);                
            }).should.throwError();
        })
        it('should return a RecordSchema if an object is passed with a type "record"', function(){
            var schema = Avro.Schema({ 
                name: "myrecord", 
                type: "record", 
                fields: [
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
                ]
            });
            schema.should.be.an.instanceof(Avro.RecordSchema);
            schema.type.should.equal("record");
            schema.fields.should.be.an.instanceof(Object);
            _.size(schema.fields).should.equal(3);
        });
        it('should return a MapSchema if an object is passed with a type "map"', function(){
            var schema = Avro.Schema({
                "name": "mapSchemaTest", 
                "type": {
                    "type": "map", 
                    "values": "bytes"
                }
            });
            schema.should.be.an.instanceof(Avro.MapSchema);
            schema.values.should.be.an.instanceof(Avro.PrimitiveSchema);
            schema.values.type.should.equal("bytes");
            schema.type.should.equal("map");
        });
        it('should return an ArraySchema is an object is passed with a type "array"', function(){
            var schema = Avro.Schema({
                "name": "arraySchemaTest",
                "type": "array",
                "items": "long"
            });
            schema.should.be.an.instanceof(Avro.ArraySchema);
            schema.items.should.be.an.instanceof(Avro.PrimitiveSchema);
            schema.type.should.equal("array");
        });
        it('should return a FixedSchema if an object is passed with a type "fixed"', function(){
            var schema = Avro.Schema({
                "name": "fixedSchemaTest", 
                "type": {
                    "type": "fixed", 
                    "size": 50
                }
            });
            schema.should.be.an.instanceof(Avro.FixedSchema);
            schema.size.should.equal(50);
            schema.type.should.equal("fixed");
        });
        it('should return a EnumSchema if an object is passed with a type "enum"', function(){
            var schema = Avro.Schema({
                "type": "enum",
                "symbols": [ "Alpha", "Bravo", "Charlie", "Delta"]
            });
            schema.should.be.an.instanceof(Avro.EnumSchema);
            schema.symbols.should.have.length(4);
            schema.type.should.equal("enum");
        })
    })
});