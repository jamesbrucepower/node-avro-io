var assert = require("assert");
var should = require('should');
var avro = require(__dirname + "/avro");
var validator = require(__dirname + "/validator").Validator;
var fs = require("fs");

describe('DataFile', function(){
    describe('writeFile()', function() {
        var schema = JSON.parse(fs.readFileSync(__dirname + "../target/generated-sources/avro/acs.avsc", 'utf8'));
        schema.should.exist();
    });
    describe('readFile()', function() {
        
    });
})