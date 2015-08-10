Node Avro IO
============

[![Build Status](https://secure.travis-ci.org/jamesbrucepower/node-avro-io.png)](http://travis-ci.org/jamesbrucepower/node-avro-io)

Implements the [avro spec](http://avro.apache.org/docs/current/spec.html)

This status of this repository is *initial release*

```bash
npm install node-avro-io
```

or

```bash
npm install git://github.com/jamesbrucepower/node-avro-io.git
```

Serializing data to an avro binary file
```
var avro = require('./index').DataFile.AvroFile();
var schema = {
            "name": "data",
            "type": "record",
            "fields": [
                {"name":"key","type": "string"},
                {"name":"value","type": "string"},
                {"name":"flag","type": "boolean"},
                {"name":"subrecord","type":"record","fields":[
                    {"name":"key","type":"string"},
                    {"name":"value","type":["string","int","null"]}
                ]}
            ]
};
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer
    .append({ key:"john", value:"hive", flag: true, subrecord: { key: "preference", value: 2}})
    .append({ key:"eric", value:"lola", flag: true, subrecord: { key: "postcode", value: null}})
    .end({ key:"fred", value:"wonka", flag: false, subrecord: { key: "city", value: "michigan"}});
```

Deserializing data to from avro binary file
```
require("./index").DataFile.AvroFile()
	.open('test.avro', null, { flags: 'r' })
		.on('data', function(data) {
	  		console.log(data);
		});
```

See tests for more usage examples

TODO:

- Avro RPC
- Support for Trevni (column major data serialization)
