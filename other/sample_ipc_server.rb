#!/usr/bin/env ruby
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'socket'
require 'avro'

LOG_EVENT_PROTOCOL_JSON = <<-EOS
{
  "protocol" : "LoggingProtocol",
  "namespace" : "uk.co.newsint.platform.logging.avro",
  "types" : [ {
    "type" : "enum",
    "name" : "Status",
    "symbols" : [ "OK", "FAILED", "UNKNOWN" ]
  }, {
    "type" : "record",
    "name" : "AppException",
    "fields" : [ {
      "name" : "class",
      "type" : "string"
    }, {
      "name" : "message",
      "type" : "string"
    }, {
      "name" : "stackTrace",
      "type" : [ "null", "string" ]
    } ]
  }, {
    "type" : "record",
    "name" : "Request",
    "fields" : [ {
      "name" : "headers",
      "type" : {
        "type" : "map",
        "values" : "string"
      }
    }, {
      "name" : "method",
      "type" : "string"
    }, {
      "name" : "path",
      "type" : "string"
    }, {
      "name" : "queryString",
      "type" : [ "string", "null" ]
    }, {
      "name" : "remoteIp",
      "type" : "string"
    }, {
      "name" : "body",
      "type" : {
        "type" : "map",
        "values" : "string"
      }
    } ]
  }, {
    "type" : "record",
    "name" : "Response",
    "fields" : [ {
      "name" : "status",
      "type" : "int"
    }, {
      "name" : "headers",
      "type" : {
        "type" : "map",
        "values" : "string"
      }
    }, {
      "name" : "body",
      "type" : {
        "type" : "map",
        "values" : "string"
      }
    } ]
  }, {
    "type" : "record",
    "name" : "Customer",
    "fields" : [ {
      "name" : "data",
      "type" : {
        "type" : "map",
        "values" : "string"
      }
    } ]
  }, {
    "type" : "record",
    "name" : "AccessLogEvent",
    "fields" : [ {
      "name" : "host",
      "type" : "string"
    }, {
      "name" : "time",
      "type" : "string"
    }, {
      "name" : "elapsedTime",
      "type" : "long"
    }, {
      "name" : "tid",
      "type" : "int"
    }, {
      "name" : "request",
      "type" : "Request"
    }, {
      "name" : "response",
      "type" : "Response"
    }, {
      "name" : "customer",
      "type" : "Customer"
    }, {
      "name" : "exception",
      "type" : [ "AppException", "null" ]
    } ]
  } ],
  "messages" : {
    "append" : {
      "request" : [ {
        "name" : "event",
        "type" : "AccessLogEvent"
      } ],
      "response" : "Status"
    },
    "appendBatch" : {
      "request" : [ {
        "name" : "events",
        "type" : {
          "type" : "array",
          "items" : "AccessLogEvent"
        }
      } ],
      "response" : "Status"
    }
  }
}
EOS

LOGEVENT_PROTOCOL = Avro::Protocol.parse(LOG_EVENT_PROTOCOL_JSON)

class LogResponder < Avro::IPC::Responder
  def initialize
    super(LOGEVENT_PROTOCOL)
  end

  def call(message, request)
    if message.name == 'append'
        request.inspect
    end
  end
end

class RequestHandler
  def initialize(address, port)
    @ip_address = address
    @port = port
  end

  def run
    server = TCPServer.new(@ip_address, @port)
    while (session = server.accept)
      handle(session)
      session.close
    end
  end
end

class LogHandler < RequestHandler
  def handle(request)
    responder = LogResponder.new()
    transport = Avro::IPC::SocketTransport.new(request)
    str = transport.read_framed_message
    transport.write_framed_message(responder.respond(str))
  end
end

if $0 == __FILE__
  handler = LogHandler.new('localhost', 9090)
  puts "running..."
  handler.run
end
