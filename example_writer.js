var avro = require('./index').DataFile.AvroFile();
var schema = { type: "string" };
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer
    .append("The quick brown fox jumped over the lazy dogs")
    .append("Another entry")
    .end("last one");
