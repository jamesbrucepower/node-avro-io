require("./index").DataFile.AvroFile()
	.open('test.avro', null, { flags: 'r' })
		.on('data', function(data) {
	  		console.log(data);
		});
