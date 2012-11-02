var assert = require("assert");
var should = require("should");
var fs = require("fs");

var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var DataFile = require(libpath + "/../lib/datafile");

describe('DataFile', function(){
    var testFile = __dirname + "/../test/data/test.avro";
    var dataFile;
    before(function(){
        dataFile = DataFile();
        if (fs.existsSync(testFile))
            fs.unlinkSync(testFile);
    });
    after(function(){
        fs.unlinkSync(testFile);
    })
    describe('open()', function(){
        it('should open a file for writing if passed a w flag and write an avro header', function(done){
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'w' });
            dataFile.Writer.write(1, function(err) {
                dataFile.close();
                fs.existsSync(testFile).should.be.true;                
                done();
            });
        });
        it('should open a file for reading if passed a r flag', function(done){
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'r' });
            dataFile.Reader.read(function(err, data) {
                if (err) done(err);
                else {
                    data.should.equal(1);
                    fs.unlinkSync(testFile); 
                    done();               
                }
            });
        });
        it('should throw an error if an unsupported codec is passed as an option', function(){
            (function() {
                dataFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        })
    });
    describe('Writer()', function(){
        describe('generateSyncMarker()', function(){
            it('should generate a 16 byte sequence to be used as a marker', function(){
                dataFile.Writer.generateSyncMarker(16).length.should.equal(16);
            });
        });
        describe('write()', function() {
            it('should write a schema and associated data to a file', function(done) {
                var schema = "string";  //{ "type": "string" };
                var data = "The quick brown fox jumped over the lazy dogs";
                dataFile.open(testFile, schema, { flags: 'w', codec: "deflate" });
                dataFile.Writer.write(data, function(err) {
                    dataFile.Writer.write(data, function(err) {
                        dataFile.Writer.write(data, function(err) {
                            dataFile.close();
                            fs.existsSync(testFile).should.be.true; 
                            done();                       
                        });                  
                    });                
                });
            });
        });
    })
    describe('Reader()', function(){
        describe('read()', function() {
            it('should read an avro data file', function(done){
                var schema = { "type": "string" };
                dataFile.open(testFile, schema, { flags: 'r' });
                var i = 0;
                dataFile.Reader.read(function(err, data) {
                    data.should.equal("The quick brown fox jumped over the lazy dogs");
                    i++;
                    if (i == 3) {
                        dataFile.close();
                        done();
                    }
                });
            });
        });      
    });
})