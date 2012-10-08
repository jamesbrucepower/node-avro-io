require 'rubygems'
require 'avro'
require 'test/unit'
require 'json'

class TestACSFlumeAvro < Test::Unit::TestCase

    def setup
        @PROTOCOL_FILE = "../target/generated-sources/avro/acs.avpr"
        @SCHEMA_FILE = "../target/generated-sources/avro/acs.avsc"
        @AVRO_FILE = "test.avro"
        @schema = Avro::Schema.parse(File.read(@SCHEMA_FILE))
        @data = { 
            "type" => "ACCESS",
            "host" => "a-fake-host.domain.name", 
            "time" => "2012-01-01T00:00:00.000Z",
            "elapsedTime" => {
              "long" => 1902961592
            },
            "tid" => 1209,
            "message" => {
              "string" => "test"
            },
            "request" => {
                "uk.co.newsint.platform.logging.avro.Request" => {
                  "headers" => {
                      "X-NI-apiKey" => "19aa8f981202938afhlj18a9f8s",
                      "User-Agent" => "firefox"
                  },
                  "method" => "POST",
                  "path" => "/authN/authenticate",
                  "queryString" => "params=1&topic=news",
                  "remoteIp" => "10.198.12.58",
                  "body" => {
                      "username" => "bob@aol.com",
                      "password" => "topsecret",
                      "tenantId" => "TNL"
                  }
                }
            },
            "response" => {
                "uk.co.newsint.platform.logging.avro.Response" => {
                  "status" => 200,
                  "headers" => {
                  }, 
                  "body" => {
                  }
                }
            },
            "user" => {
                "uk.co.newsint.platform.logging.avro.User" => {
                  "username" => "bob@aol.com",
                  "externalId" => {
                    "string" => "F8982918G12"
                  }
                }
            }
        }
    end

    def testWriteAvroToDisk
        file = File.open(@AVRO_FILE, 'wb')
        writer = Avro::IO::DatumWriter.new(@schema);
        dw = Avro::DataFile::Writer.new(file, writer, @schema)
        dw << @data
        dw.close
    end
 
    def testReadAvroFromDisk
        file = File.open(@AVRO_FILE, 'r+')
        dr = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
        dr.each { |record| 
            assert_equal(record["elapsedTime"], @data["elapsedTime"])
            puts record.class
            puts @data.class
            puts JSON.pretty_generate(record) 
        }
    end
    
    def testReadAvroUsingSchemaFromDisk
        file = File.open(@AVRO_FILE, 'r+')
        reader = Avro::IO::DatumReader.new(nil, Avro::Schema.parse(@schema))
        dr = Avro::DataFile::Reader.new(file, reader)
        dr.each { |record| p record }
    end
    
    def testSendingAvro
        server_address = "localhost"
        port = 33443
        protocol = Avro::Protocol.parse(File.read(@PROTOCOL_FILE));
        sock = TCPSocket.new(server_address, port)
        client = Avro::IPC::SocketTransport.new(sock)
        requestor = Avro::IPC::Requestor.new(protocol, client)
        requestor.request("append", @data)
        sock.close
    end

end
