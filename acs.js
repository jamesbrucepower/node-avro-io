var DataFile = require("./index").DataFile;

var avro = DataFile.AvroFile();

var reader = avro.open('/users/jpower1/ni-acs-prod-auth-i-d0e47e9b.cloud-newsint.co.uk.1352966423873', null, { flags: 'r' });
reader.on('data', function(data) {
    console.log(data);
});
