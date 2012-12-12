var DataFile = require("./index").DataFile;

var avro = DataFile.AvroFile();

var reader = avro.open('test/data/test.writer.avro.random', null, { flags: 'r' });
reader.on('data', function(data) {
    console.log(data);
});
