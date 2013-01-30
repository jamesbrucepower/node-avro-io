var util = require('util');

var AvroIOError = function() { 
    Error.call(this);    
    this.name = 'Avro IO Error';
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};   

var AvroFileError = function() { 
    Error.call(this);    
    this.name = 'Avro File Error';
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};   
    
var AvroBlockError = function() { 
    Error.call(this);    
    this.name = 'Avro Block Error';
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};   

var AvroBlockDelayReadError = function() { 
    Error.call(this);    
    this.name = 'Avro Block Delay Read Error';
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};  

var AvroInvalidSchemaError = function() { 
    Error.call(this);    
    this.name = 'Avro Invalid Schema Error';
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};  

util.inherits(AvroIOError, Error);
util.inherits(AvroFileError, Error);
util.inherits(AvroBlockError, Error);
util.inherits(AvroBlockDelayReadError, Error);
util.inherits(AvroInvalidSchemaError, Error);

exports.BlockDelayReadError = AvroBlockDelayReadError;
exports.BlockError = AvroBlockError;
exports.FileError = AvroFileError;
exports.InvalidSchemaError = AvroInvalidSchemaError;
exports.IOError = AvroIOError;