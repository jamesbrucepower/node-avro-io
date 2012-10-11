var assert = require("assert");
var should = require("should");
var fs = require("fs");

var DataFile = require(__dirname + "/../datafile");

describe('DataFile', function(){
    var testFile;
    before(function(){
        testFile = __dirname + "/../data/test.avro";
    });
    after(function(){
        //fs.unlinkSync(testFile);
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function() {
            var schema = { "type": "string" };
            var data = "the quick brown fox jumped over the lazy dogs";
            var dataFile = DataFile();
            dataFile.open(testFile, schema, { flags: 'w' });
            dataFile.write(data);
            dataFile.write(data);
            dataFile.write(data);
            dataFile.close();
            fs.existsSync(testFile).should.be.true;
        });
    });
    describe('read()', function() {
        it('should read an avro data file', function(done){
            var schema = { "type": "string" };
            var dataFile = DataFile()
            dataFile.open(testFile, schema, { flags: 'r' });
            dataFile.read(function(err, data) {
                data.should.equal("the quick brown fox jumped over the lazy dogs");
                done();
            });
        });
    });
})