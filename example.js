var fs = require('fs');
var DataFile = require('./index.js').DataFile;

var avro = DataFile.AvroFile();
var fileStream = fs.createWriteStream('test.avro');

var schema = 'string';
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer.pipe(fileStream);
writer
    .append("The quick brown fox jumped over the lazy dogs")
    .append("Another entry")
    .end();
