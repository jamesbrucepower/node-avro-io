var fs = require('fs');
var DataFile = require('./index').DataFile;

var avro = DataFile.AvroFile();
var fileStream = fs.createWriteStream('test.avro');

var schema = { type: "string" };
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'null' });
writer.pipe(fileStream);
writer
    .append("The quick brown fox jumped over the lazy dogs")
    .append("Another entry")
    .end("last one");
