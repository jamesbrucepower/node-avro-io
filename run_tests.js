"use strict";
var Mocha = require('mocha'),
    fs = require('fs'),
    path = require('path');

var mocha = new Mocha();

var curDir = __dirname;
var testDir = path.join(curDir, 'test');
fs.readdirSync(testDir).filter(function(file){
    // Only keep the .js files
    return file.substr(-3) === '.js';

}).forEach(function(file){
    // Use the method "addFile" to add the file to mocha
    mocha.addFile(
        path.join(testDir, file)
    );
});

// Now, you can run the tests.
mocha.run(function(failures){
    process.on('exit', function () {
        process.exit(failures);
    });
});