var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';
var fs = require('fs');
var should = require('should');
require('buffertools');
var DataFile = require(libpath + 'datafile');
describe('AvroFile', function(){
    var testFile = __dirname + "/../test/data/test.avrofile.avro";
    var avroFile;
    before(function(){
        avroFile = DataFile.AvroFile();
        if (fs.existsSync(testFile))
            fs.unlinkSync(testFile);
    });
    after(function(){
        fs.unlinkSync(testFile);
    });
    describe('open()', function(){
        it('should open a file for writing and return a writer', function(done){
            var schema = "string";
            var writer = avroFile.open(testFile, schema, { flags: 'w' });
            writer.should.be.an.instanceof(DataFile.Writer);
            writer.write("testing", function(err) {
                should.not.exist(err);
                avroFile.close(function() {
                    fs.existsSync(testFile).should.be.true;
                    done();
                });
            });
        });
        it('should open a file for reading and return a reader', function(done){
            var schema = "string";
            var reader = avroFile.open(testFile, schema, { flags: 'r' });
            reader.should.be.an.instanceof(DataFile.Reader);
            reader.read(schema, function(err, data) {
                should.not.exist(err);
                data.should.equal("testing");
                done();
            });
        });
        it('should throw an error if an unsupported codec is passed as an option', function(){
            (function() {
                avroFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        });
        it('should throw an error if an unsupported operation is passed as an option', function(){
            (function() {
                avroFile.open(null, null, { flags: 'x'});
            }).should.throwError();
        });
    });
    describe('close()', function(){
          it('should close a file for the current operation', function(done){
            var schema = "string";
            var writer = avroFile.open(testFile, schema, { flags: 'w' });
            writer.should.be.an.instanceof(DataFile.Writer);
            writer.write("testing close", function(err) {
                should.not.exist(err);
                (function() {
                    fs.writeSync(writer.fd, new Buffer([0x50, 0x60]), 0, 2);
                }).should.not.throwError();
                avroFile.close(function() {
                    fs.existsSync(testFile).should.be.true;
                    (function() {
                        fs.writeSync(writer.fd, new Buffer([0x50, 0x60]), 0, 2);
                    }).should.throwError();
                    done();
                });
            });
          })
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
    describe('skip()', function(){
        it('should skip n bytes of the block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.skip(3);
            block.offset.should.equal(3);
            block.skip(2);
            block.offset.should.equal(5);
        });
        it('should throw an error if you try to skip past the end of the written amount', function(){
            (function() {
                var block = new DataFile.Block(32);
                block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                block.skip(7);                
            }).should.throwError();
        });
    })
    describe('slice()', function(){
        it('should return a the written part of the Block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.slice().equals(new Buffer([0x01, 0x02, 0x03, 0x04, 0x05, 0x06])).should.be.true;
        });
        it('should return the specified sub section of a block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.slice(2,5).equals(new Buffer([0x03, 0x04, 0x05])).should.be.true;          
        })
    })
    describe('toBuffer()', function(){
        it('should return a buffer with the contents of the block', function(){
            var block = new DataFile.Block(64);
            block.write([0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71]);
            block.isEqual([0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71]).should.be.true;
        });
    });
});
describe('Writer()', function(){
    var avroFile;
    var testFile = __dirname + "/../test/data/test.writer.avro";
    beforeEach(function(){
        avroFile = DataFile.AvroFile();
    });
    describe('_generateSyncMarker()', function(){
        it('should generate a 16 byte sequence to be used as a marker', function(){
            var writer = DataFile.Writer();
            should.not.exist(writer._generateSyncMarker(-5));
            should.not.exist(writer._generateSyncMarker(0));
            writer._generateSyncMarker(16).length.should.equal(16);
            writer._generateSyncMarker(2).length.should.equal(2);
        });
    });
    describe('compressData()', function(){
        it('should compress a given buffer with deflate and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
              var writer = DataFile.Writer();
            writer.compressData(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65]), "deflate", function(err, data) {
                data.equals(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00])).should.be.true;
                reader.decompressData(data, "deflate", function(err, data) {
                    data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
                      done();
                })
              })
        });
/*        it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            var writer = DataFile.Writer();
            writer.compressData(new Buffer("compress this text"), "snappy", function(err, data) {
                console.log(data);
                done();
                //data.equals(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00])).should.be.true;
                //reader.decompressData(data, "snappy", function(err, data) {
                //    data.toString().should.equal("compress this text");
                //    done();
                //})
              })
        });
  */      it('should return an error if an unsupported codec is passed as a parameter', function(done){
            var writer = DataFile.Writer();
            writer.compressData(new Buffer([0x13, 0x55, 0x35, 0x75]), "unsupported", function(err, data) {
                should.exist(err);
                err.should.be.an.instanceof(Error);
                done();
            });
        });
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function(done) {
            var schema = "string";  //{ "type": "string" };
            var data = "The quick brown fox jumped over the lazy dogs";
            var writer = avroFile.open(testFile, schema, { flags: 'w', codec: "deflate" });
            writer.write(data, function(err) {
                should.not.exist(err);
                writer.write(data, function(err) {
                    should.not.exist(err);
                    writer.write(data, function(err) {
                        should.not.exist(err);
                        avroFile.close(function() {
                            fs.existsSync(testFile).should.be.true;
                            done();
                        });
                    });
                });
            });
        });
    });
});
describe('Reader()', function(){
    var avroFile;
    var testFile = __dirname + "/../test/data/test.writer.avro";
    beforeEach(function(){
        avroFile = DataFile.AvroFile();
    });
    describe('decompressData()', function(){
        it('should compress a given buffer with deflate and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "deflate", function(err, data) {
                data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
                done();
            });
        });
        /*it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "snappy", function(err, data) {
                data.equals(new Buffer([0xe4, 0x21, 0xe1, 0x40, 0xc6, 0xd6, 0xf1, 0x11, 0x4c, 0xd5, 0x06, 0x64, 0x0a])).should.be.true;
                done();
            });
        });*/
        it('should just return the same data if the codec is null', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "null", function(err, data) {
                data.equals(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00])).should.be.true;
                done();
            });
        });
        it('should return an error if an unsupported codec is passed as a parameter', function(done) {
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75]), "unsupported", function(err, data) {
                should.exist(err);
                err.should.be.an.instanceof(Error);
                done();
            });
        })
    })
    describe('read()', function() {
        it('should read an avro data file', function(done){
            var schema = { "type": "string" };
            var reader = avroFile.open(testFile, schema, { flags: 'r' });
            reader.should.be.an.instanceof(DataFile.Reader);
            var i = 0;
            reader.read(schema, function(err, data) {
                should.not.exist(err);
                data.should.equal("The quick brown fox jumped over the lazy dogs");
                i++;
                if (i == 3) {
                    avroFile.close(function() {
                        fs.unlinkSync(testFile);
                        done();
                    });
                }
            });
        });
    });
});
