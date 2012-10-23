var DataFile = require(__dirname + "/lib/datafile");

dataFile = DataFile();
dataFile.open("ni.avro");
//dataFile.open("test/data/ni-acs-uat-auth-i-0a912141.cloud-newsint.co.uk.1350640812592");
dataFile.read(function(err, data) {
    if (err) 
        console.error(err);
    else
        console.log(JSON.stringify(data, 0, 4));
});
