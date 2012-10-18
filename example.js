var DataFile = require(__dirname + "/lib/datafile");

dataFile = DataFile();
dataFile.open("test/data/acs.avro", "int", { flags:"r"});
dataFile.read(function(err, data) {
    if (err) 
        console.error(err);
    else
        console.log("%j", data);
});