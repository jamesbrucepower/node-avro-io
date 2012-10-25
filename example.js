var sprint = require('sprint');
var DataFile = require(__dirname + "/lib/datafile");

var fileName = process.argv[2];
dataFile = DataFile();
dataFile.open(fileName);
dataFile.Reader.read(function(err, data) {
    if (err) 
        console.error(err);
    else {
        if (data.elapsedTime > 5E8)
            console.log("%dms: %s", Math.floor(data.elapsedTime/1E6), data.request.path);
        //
        //if (data.request.path === "/authZ/authorize" && data.response.status == 200)
          //  console.log(sprint("%30s: %s", data.customer.data.username, data.request.headers["user-agent"]));
    }
});
