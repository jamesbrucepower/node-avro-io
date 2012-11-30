Node Avro IO
============

Implements the [avro spec](http://avro.apache.org/docs/current/spec.html)

This status of this repository is "work in progress"

```bash
npm install https://github.com/jamesbrucepower/node-avro-io.git
```

Serializing data to an avro binary file
```
var DataFile = require("node-avro-io").DataFile;

var avro = DataFile.AvroFile();

var schema = "string";
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer.write("The quick brown fox jumped over the lazy dogs", function(err) {
    avro.close();
});
```

Deserializing data to from avro binary file
```
var DataFile = require("node-avro-io").DataFile;

var avro = DataFile.AvroFile();

var schema = "string";
var reader = avro.open("test.avro", { flags: 'r' });
reader.read(null, function(err, data) {
    console.log(data);
});
```
...lots more to follow...

For now see test/*

TODO:

- Avro RPC
- Support for Trevni (column major data serialization)
- Fix snappy compression support
