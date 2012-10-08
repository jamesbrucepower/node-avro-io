var assert = require("assert");
var should = require('should');
var avro = require("./avro");
var validator = require("./validator").Validator;
var fs = require("fs");

describe('Avro', function(){
    before(function(){
        
    });
    describe('#writeAvro()', function() {
        it('should create an avro data file', function(){
            var schema = JSON.parse(fs.readFileSync("../target/generated-sources/avro/acs.avsc", 'utf8'));
            var logEvent = { 
                "type": "ACCESS",
                "host": "a-fake-host.domain.name", 
                "time": "2012-01-01T00:00:00.000Z",
                "elapsedTime": {
                    "long": 1902961592,
                },
                "tid": 1209,
                "message": {
                    "string": "test",
                },
                "request": {
                    "uk.co.newsint.platform.logging.avro.Request": {
                        "headers": {
                            "X-NI-apiKey": "19aa8f981202938afhlj18a9f8s",
                            "User-Agent": "firefox"
                        },
                        "method": "POST",
                        "path": "/authN/authenticate",
                        "queryString": "params=1&topic=news",
                        "remoteIp": "10.198.12.58",
                        "body": {
                            "username": "bob@aol.com",
                            "password": "topsecret",
                            "tenantId": "TNL"
                        }
                    }
                },
                "response": {
                    "uk.co.newsint.platform.logging.avro.Response": {
                        "status": 200,
                        "headers": {
                        }, 
                        "body": {
                        }
                    }
                },
                "user": {
                    "uk.co.newsint.platform.logging.avro.User": {
                        "username": "bob@aol.com",
                        "externalId": "F8982918G12"
                    }
                }, 
                exception: null
            }
            console.log(validator.validate(schema, logEvent));
            console.log(avro.validate(schema, logEvent));
            //var data = avro.encode(schema, logEvent);
            //console.log(data);
            //fs.writeFileSync("output.avro", data, 'utf8');
        })
    })
    describe('#readAvro()', function(){
        it('should read an avro data file', function(done) {
            var schema = JSON.parse(fs.readFileSync("../target/generated-sources/avro/acs.avsc", 'utf8'));
            schema.should.exist;
            var avroData = fs.readFileSync("test.avro");
            var validation = avro.validate(schema, avroData);
            console.log(avro.decode(schema, avroData));
            validation.should.be.true;
        })
    })
})
