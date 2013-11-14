var avro = require('./index').DataFile.AvroFile();
var schema = {
            "name": "data",
            "type": "record",
            "fields": [
                {"name":"key","type": "string"},
                {"name":"value","type": "string"},
                {"name":"flag","type": "boolean"},
				{"name":"subrecord","type":"record","fields":[
					{"name":"key","type":"string"},
					{"name":"value","type":["string","int","null"]}
				]}
            ]
};
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer
    .append({ key:"john", value:"hive", flag: true, subrecord: { key: "preference", value: {"int":2}}})
    .append({ key:"eric", value:"lola", flag: true, subrecord: { key: "postcode", value: {"null":null}}})
    .end({ key:"fred", value:"wonka", flag: false, subrecord: { key: "city", value: {"string":"michigan"}}});
