var DataFile = require("./index").DataFile;
var Schema = require('./index').Schema;

var avro = DataFile.AvroFile();

var readSchema = Schema.Schema({
    "name": "customer",
    "type": "record",
    "fields": [{ 
        "name": "customer", 
        "type": { 
            "type":"record",
            "name":"Customer",
            "fields": [{      
                "name":"data",
                "type": {"type":"map","values":"string"}
            }]
        }
    }]
});
var reader = avro.open('/users/jpower1/ni-acs-prod-auth-i-d0e47e9b.cloud-newsint.co.uk.1352966423873', readSchema, { flags: 'r' });
reader.on('data', function(data) {
    console.log(data);
    //if (data.response.status === 200 && data.request.path === '/authZ/authorize')
        //console.log(data);
//        console.log(data.customer.data.username);
});
