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
var fs = require('fs');
var DataFile = require('node-avro-io').DataFile;

var avro = DataFile.AvroFile();
var fileStream = fs.createFileStream('test.avro');

var schema = 'string';
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer
    .pipe(fileStream)
    .append("The quick brown fox jumped over the lazy dogs")
    .append("Another entry")
    .end();
```

Deserializing data to from avro binary file
```
var DataFile = require("node-avro-io").DataFile;

var avro = DataFile.AvroFile();

var reader = avro.open('test.avro', { flags: 'r' });
reader.on('data', function(data) {
    console.log(data);
});
```
...lots more to follow...

For now see test/*

TODO:

- Avro RPC
- Support for Trevni (column major data serialization)
