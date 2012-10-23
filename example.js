var DataFile = require(__dirname + "/lib/datafile");

var fileName = process.argv[2];
dataFile = DataFile();
dataFile.open(fileName);
dataFile.read(function(err, data) {
    if (err) 
        console.error(err);
    else
        console.log(JSON.stringify(data, 0, 4));
});
