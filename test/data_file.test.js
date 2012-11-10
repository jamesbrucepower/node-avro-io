var assert = require("assert");
var should = require("should");
var fs = require("fs");
require('buffertools');

var libpath = process.env["MOCHA_COV"] ? __dirname + "/../lib-cov/" : __dirname + "/../lib/";
var DataFile = require(libpath + "/../lib/datafile");

describe('AvroFile', function(){
    var testFile = __dirname + "/../test/data/test.avro";
    describe('open()', function(){
	    before(function(){
	        if (fs.existsSync(testFile))
	            fs.unlinkSync(testFile);
	    });
	    after(function(){
	        //fs.unlinkSync(testFile);
	    })
	    it('should open a file for writing if passed a w flag and write an avro header', function(done){
            var schema = "int";
            var writer = DataFile.AvroFile.open(testFile, schema, { flags: 'w' });
            writer.write(1, function(err) {
                DataFile.close();
                fs.existsSync(testFile).should.be.true;                
                done();
            });
        });
        it('should open a file for reading if passed a r flag', function(done){
            var schema = "int";
            var reader = DataFile.AvroFile.open(testFile, schema, { flags: 'r' });
            reader.read(function(err, data) {
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
                DataFile.AvroFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        })
    });
	
	describe('Block()', function(){
		describe('length', function() {
			it('should return the current length of a Block', function(){
				var block = new DataFile.Block();
				block.length.should.equal(0);
				block.write(0x10);
				block.length.should.equal(1);
			});
		});
		describe('flush()', function(){
			it('should flush the buffer by setting the offset of 0', function(){
				var block = new DataFile.Block();
				block.write(0x55);
				block.flush();
				block.length.should.equal(0);
			});
		});
		describe('write()', function(){
			it('should write a single byte into the buffer', function(){
				var block = new DataFile.Block();
				block.write(0x20);
				block.isEqual([0x20]).should.be.true;
			});
			it('should write an array of bytes into the buffer', function() {
				var block = new DataFile.Block();
				var bArray = [0x10, 0x20, 0x30, 0x40, 0x50, 0x60];
				block.write(bArray);
				block.isEqual(bArray).should.be.true;
			})
		});
		describe('value()', function(){
			it('should return a buffer with the contents of the block', function(){
				var block = new DataFile.Block();
				block.write([0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71]);
				block.toBuffer().equals(new Buffer([0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71])).should.be.true;
			})
		})
	});
	
    describe('Writer()', function(){
        var avroFile;
		beforeEach(function(){
            avroFile = DataFile.AvroFile();
		})
        describe('_generateSyncMarker()', function(){
            it('should generate a 16 byte sequence to be used as a marker', function(){
                var writer = DataFile.Writer();
                writer._generateSyncMarker(16).length.should.equal(16);
            });
        });
		describe('compressData()', function(){
			it('should compress a given buffer with deflate and return the compressed buffer', function(done){
				var reader = DataFile.Reader();
			  	var writer = DataFile.Writer();
                writer.compressData(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65]), "deflate", function(err, data) {
					reader.decompressData(data, "deflate", function(err, data) {
						data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
				  		done();
					})
			  	})
			})
		})
        describe('write()', function() {
            it('should write a schema and associated data to a file', function(done) {
                var schema = "string";  //{ "type": "string" };
                var data = "The quick brown fox jumped over the lazy dogs";
                var writer = avroFile.open(testFile, schema, { flags: 'w', codec: "deflate" });
                writer.write(data, function(err) {
                    writer.write(data, function(err) {
                        writer.write(data, function(err) {
                            avroFile.close();
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
                DataFile.open(testFile, schema, { flags: 'r' });
                var i = 0;
                DataFile.Reader.read(function(err, data) {
                    data.should.equal("The quick brown fox jumped over the lazy dogs");
                    i++;
                    if (i == 3) {
                        DataFile.close();
                        done();
                    }
                });
            });
        });      
    });
})