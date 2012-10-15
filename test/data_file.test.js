var assert = require("assert");
var should = require("should");
var fs = require("fs");

var DataFile = require(__dirname + "/../lib/datafile");

describe('DataFile', function(){
    var testFile = __dirname + "/../test/data/test.avro";
    before(function(){
        if (fs.existsSync(testFile))
            fs.unlinkSync(testFile);
    });
    describe('open()', function(){
        it('should open a file for writing if passed a w flag and write an avro header', function(){
            var dataFile = DataFile();
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'w' });
            dataFile.write(1, function(err) {
                dataFile.close();
                fs.existsSync(testFile).should.be.true;                
            });
        });
        it('should open a file for reading if passed a r flag', function(){
            var dataFile = DataFile();
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'r' });
            var data = dataFile.read();
            dataFile.close();
            data.should.equal(1);
            fs.unlinkSync(testFile);
        });
        it('should throw an error if an unsupported codec is passed as an option', function(){
            var dataFile = DataFile();
            (function() {
                dataFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        })
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function() {
            var schema = "string";  //{ "type": "string" };
            var data = "The quick brown fox jumped over the lazy dogs";
            var dataFile = DataFile();
            dataFile.open(testFile, schema, { flags: 'w', codec: "deflate" });
            dataFile.write(data, function(err) {
                dataFile.write(data, function(err) {
                    dataFile.write(data, function(err) {
                        dataFile.close();
                        fs.existsSync(testFile).should.be.true;                        
                    });                  
                });                
            });
        });
    });
    describe('read()', function() {
        it('should read an avro data file', function(done){
            var schema = { "type": "string" };
            var dataFile = DataFile()
            dataFile.open(testFile, schema, { flags: 'r' });
            dataFile.read(function(err, data) {
                data.should.equal("The quick brown fox jumped over the lazy dogs");
                done();
            });
        });
    });
})