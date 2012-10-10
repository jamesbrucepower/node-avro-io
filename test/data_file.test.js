var assert = require("assert");
var should = require("should");
var fs = require("fs");

var DataFile = require(__dirname + "/../data_file");

describe('DataFile', function(){
    var testFile;
    before(function(){
        testFile = __dirname + "/../data/test.avro";
    });
    after(function(){
        fs.unlinkSync(testFile);
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function(done) {
            var schema = { "type": "string" };
            var data = "the quick brown fox jumped over the lazy dogs";
            console.error(Object.getOwnPropertyNames(DataFile));
            var dataFile = DataFile.open(testFile, "w", schema);
            dataFile.write(data, null, function(err) {
                fs.existsSync(testFile).should.be.true;
                done();
            });        
        });
    });
    describe('read()', function() {
        it('should read an avro data file', function(done){
            var schema = { "type": "string" };
            var dataFile = DataFile.open(testFile, "r", schema);
            dataFile.read(function(err, data) {
                data.should.equal("the quick brown fox jumped over the lazy dogs");
                done();
            });
        });
    });
})