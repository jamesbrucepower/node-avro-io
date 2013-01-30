var util = require('util');

var AbstractAvroError = function (constr, args) {
    Error.call(this);
    Error.captureStackTrace(this, constr || this);
//    this.name = constr.prototype.name || this.constructor.name;
    console.error(args);
    this.message = util.format.apply(null, args); 
}
util.inherits(AbstractAvroError, Error);
AbstractAvroError.prototype.name = 'AbstractAvroError'

var AvroIOError = function() { 
    AvroIOError.super_.call(this, this.constructor, arguments);
};   
util.inherits(AvroIOError, AbstractAvroError);
AvroIOError.prototype.message = 'AvroIOError';

// Error objects
var AvroFileError = function() { 
    AvroFileError.super_.call(this, this.constructor, arguments);
};   
util.inherits(AvroFileError, AbstractAvroError);
AvroFileError.prototype.message = 'AvroFileError';
    
var AvroBlockError = function() {
    AvroBlockError.super_.call(this, this.constructor, arguments);
};
util.inherits(AvroBlockError, AbstractAvroError);
AvroBlockError.prototype.message = 'AvroBlockError';

var AvroBlockDelayReadError = function() {
    AvroBlockDelayReadError.super_.call(this, this.constructor, arguments);
};
util.inherits(AvroBlockDelayReadError, AbstractAvroError);
AvroBlockDelayReadError.prototype.message = 'AvroBlockDelayReadError';

var AvroInvalidSchemaError = function() { 
    AvroInvalidSchemaError.super_.call(this, this.constructor, arguments);
};
util.inherits(AvroInvalidSchemaError, AbstractAvroError);
AvroInvalidSchemaError.prototype.message = 'AvroInvalidSchemaError';

exports.BlockDelayReadError = AvroBlockDelayReadError;
exports.BlockError = AvroBlockError;
exports.FileError = AvroFileError;
exports.InvalidSchemaError = AvroInvalidSchemaError;
exports.IOError = AvroIOError;