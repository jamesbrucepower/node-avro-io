Node Avro IO
============

This status of this repository is "work in progress"

```bash
npm install git://github.com/jamesbrucepower/node-avro-io.git
```

Example usage for writing an avro file

Serializing data to an avro binary file
```
var DataFile = require("node-avro-io").DataFile;

var avro = DataFile.AvroFile();

var schema = "string";
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer.write("The quick brown fox jumped over the lazy dogs", function(err) {
    avro.close();
});

Example usage for reading an avro file
```
var DataFile = require("node-avro-io").DataFile;

var avro = DataFile.AvroFile();

var schema = "string";
var reader = avro.open("test.avro", { flags: 'r' });
reader.read(function(err, data) {
    console.log(data);
});
```

TODO:

- Avro RPC
- Support for Trevni (column major data serialization)
- Fix snappy compression support
