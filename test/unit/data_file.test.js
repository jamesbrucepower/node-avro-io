var assert = require("assert");
var should = require('should');
var fs = require("fs");

var DataFile = require(__dirname + "/../../data_file");
var validator = require(__dirname + "/../../validator").Validator;

describe('DataFile', function(){
    describe('writeFile()', function() {
        var schema = { "type": "string" };
        var outputFile = __dirname + "/../../data/test.avro";
        var dataFile = new DataFile(outputFile, schema);
        dataFile.write("testing", function(err) {
            fs.existsSync(outputFile).should.be.true;
            err.should.not.exist();
        });
    });
    describe('readFile()', function() {
        
    });
})