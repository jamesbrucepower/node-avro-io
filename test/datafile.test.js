var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';
var fs = require('fs');
var should = require('should');
require('buffertools');
var DataFile = require(libpath + 'datafile');
var Avro = require(libpath + 'schema');
var util = require('util');

var dataFile;
describe('AvroFile', function(){
    dataFile = __dirname + "/../test/data/test.avrofile.avro";
    var avroFile;
    before(function(){
        avroFile = DataFile.AvroFile();
        if (fs.existsSync(dataFile))
            fs.unlinkSync(dataFile);
    });
    after(function(){
       if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
    });
    describe('open()', function(){
        it('should open a file for writing and return a writer', function(done){
            var schema = Avro.Schema({ "type": "string" });
            var writer = avroFile.open(dataFile, schema, { flags: 'w' });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('close', function() {
                    fs.existsSync(dataFile).should.be.true;
                    done();
                });
            writer.should.be.an.instanceof(DataFile.Writer)
            writer
                .append('testing')
                .end();
        });
        it('should open a file for reading and return a reader', function(done){
            var reader = avroFile.open(dataFile, null, { flags: 'r' });
            reader.should.be.an.instanceof(DataFile.Reader);
            reader
                .on('data', function(data) {
					//console.error('data()');
                    data.should.equal("testing");
                })
                .on('error', function(err) {
					//console.error('error()');
                    if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
                    done(err);
                })
				.on('end', function() {
					//console.error('end()');
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
    dataFile = __dirname + "/../test/data/test.writer.avro";
    beforeEach(function(){
        avroFile = DataFile.AvroFile();
    });
    after(function(){
        if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
    });
    it('should write data to a file stream using a pipe', function(done){
        var schema = "string";
        var fileStream = fs.createWriteStream(dataFile);
        var writer = DataFile.Writer(schema, "null");
        writer.pipe(fileStream);
        writer
            .on('error', function(err) {
                done(err);
            })
            .on('close', function() {
                fs.existsSync(dataFile).should.be.true;
                done();
            });
        writer.append("hello world");
        writer.end();
    });      
    it('should read back data from the written file', function(done){
        var reader = DataFile.Reader();
        var fileStream = fs.createReadStream(dataFile);
        fileStream.pipe(reader);
        reader
            .on('data', function(data) {
                data.should.equal("hello world");
            })
            .on('error', function(err) {
                done(err);
            })
            .on('end', function() {
                done();
            });
    });
    function randomString() {
        var i;
        var result = "";
        var stringSize = Math.floor(Math.random() * 512);
        for (i = 0; i < stringSize; i++) 
            result += String.fromCharCode(Math.floor(Math.random() * 0xFF));
        return result;
    }
    function schemaGenerator() {
        return { 
            "testBoolean": Math.floor(Math.random() * 2) == 0 ? false : true,
            "testString": randomString(), 
            "testLong": Math.floor(Math.random() * 1E10),
            "testDouble": Math.random(),
            "testBytes": new Buffer(randomString())
        };  
    }
    it('should write a sequence marker after 16k of data to a file stream', function(done) {
        dataFile = __dirname + "/../test/data/test.writer.random.avro";
        var schema = {
            "name": "testLargeDataSet",
            "type": "record",
            "fields": [
                {"name":"testBoolean","type": "boolean"},
                {"name":"testString","type": "string"},
                {"name":"testLong","type": "long"},
                {"name":"testDouble","type": "double"},
                {"name":"testBytes","type": "bytes"}
            ]
        };
        var writer = DataFile.Writer(schema, "null");
        var fileStream = fs.createWriteStream(dataFile);
        writer.pipe(fileStream);
        writer
            .on('close', function() {
                fs.existsSync(dataFile).should.be.true;
                fs.unlinkSync(dataFile);
                done();
            })
            .on('error', function(err) {
                if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
                done(err);
            });
        var i = 0;
        var delay = 0;
        while(i++ < 20) {
            writer.append(schemaGenerator());
        }
        writer.end();
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
        it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            var writer = DataFile.Writer();
            writer.compressData(new Buffer("compress this text"), "snappy", function(err, data) {
                reader.decompressData(data, "snappy", function(err, data) {
                    if (err) done(err);
                    data.toString().should.equal("compress this text");
                    done();
                });
              });
        });
        it('should return an error if an unsupported codec is passed as a parameter', function(done){
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
            var writer = avroFile.open(dataFile, schema, { flags: 'w', codec: "deflate" });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('close', function() {
                    fs.existsSync(dataFile).should.be.true;
                    done();
                })
                .append(data)
                .append(data)
                .append(data)
                .end();
        });
        it('should write a series of integers to a file and read them back as integers', function(done) {
            aFile = __dirname + "/../test/data/test.int.avro";
            var schema = { "type": "int" };
            var writer = avroFile.open(aFile, schema, { flags: 'w', codec: "deflate" });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('close', function() {
                    fs.existsSync(aFile).should.be.true;
                    var reader = avroFile.open(aFile, null, { flags: 'r' });
                    reader.should.be.an.instanceof(DataFile.Reader);
                    var results = [];
                    reader
                        .on('data', function(data) {
                            results.push(data);
                        })
                        .on('error', function(err) {
                            console.error(err);
                            done(err);
                        })
                        .on('end', function() {
                            results.should.eql([1,14,0,552]);
                            done();
                        });
                })
                .append(1)
                .append(14)
                .append(0)
                .append(552)
                .end();
        });
    });
});
describe('Reader()', function(){

    describe('streaming', function () {
        
        it('should read a large avro data stream compressed with deflate', function(done){
            
            var count = 0;
            var fileStream = fs.createReadStream(__dirname + "/data/log.deflate.avro");

            fileStream.pipe(DataFile.Reader())
                .on('error', function(err) {
                    done(err);
                })
                .on('end', function(err) {
                    count.should.equal(4096);
                    done();
                })
                .on('header', function(data) {
                    //console.log('\nHeader\n',util.inspect(data, {colors:true, depth:null}));
                    data.should.not
                })
                .on('data', function(data) {
                    count++;
                    //console.log(data.time, data.request.path, data.request.body.rememberMe || '[]' , data.response.status);
                });
        });

        it('should read a large avro data stream compressed with snappy', function(done){
            
            var count = 0;
            var fileStream = fs.createReadStream(__dirname + "/data/log.snappy.avro");

            fileStream.pipe(DataFile.Reader())
                .on('error', function(err) {
                    done(err);
                })
                .on('end', function(err) {
                    count.should.equal(4096);
                    done();
                })
                .on('header', function(data) {
                    //console.log('\nHeader\n',util.inspect(data, {colors:true, depth:null}));
                })
                .on('data', function(data) {
                    count++;
                    //console.log(data.time, data.request.path, data.request.body.rememberMe || '[]' , data.response.status);
                });
        });    
    });
    
    describe('decompressData()', function(){
        it('should compress a given buffer with deflate and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "deflate", function(err, data) {
                data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
                done();
            });
        });
        it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x12, 0x44, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 
                                              0x73, 0x20, 0x74, 0x68, 0x69, 0x73, 0x20, 0x74, 0x65, 
                                              0x78, 0x74, 0x6c, 0x25, 0xd9, 0x04]), "snappy", function(err, data) {
                if (err) done(err);
                data.toString().should.equal("compress this text");
                done();
            });
        });
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
    describe('writing then reading', function() {
        it('should read an avro data file written and return the same data', function(done){
            
            var dataFile = __dirname + "/data/test-array-strings.avro";
            var schema = "string";
            var fileStream = fs.createWriteStream(dataFile);
            var writer = DataFile.Writer(schema);
            var source = [
                "The quick brown fox jumped over the lazy dogs", 
                "The time has come for all good men to come to the aid of...",
                "Humpty dumpty sat on the wall, humpty dumpty had a great fall..."
            ];
            writer.pipe(fileStream);
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('close', function() {

                    var fileStream = fs.createReadStream(dataFile);
                    var reader = fileStream.pipe(DataFile.Reader());

		            reader.should.be.an.instanceof(DataFile.Reader);
		            var count = 0;
		            reader
						.on('data', function(data) {
		                	data.should.equal(source[count++]);
		            	})
						.on('error', function(err) {
							console.error(err);
							done(err);
						})
                        .on('header', function(data) {
                            //console.log(data);
                        })
						.on('end', function() {
						    count.should.equal(3);
						    done();
						});
                })
                .append(source[0])
                .append(source[1])
                .append(source[2])         
                .end();
                
        });
    });
});
