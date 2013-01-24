var util = require('util');

var AbstractAvroError = function (constr, args) {
  Error.captureStackTrace(this, constr || this)
  this.message = util.format.apply(null, args); 
}
util.inherits(AbstractAvroError, Error)
AbstractAvroError.prototype.name = 'Abstract Avro Error'

var AvroIOError = function() { 
    AvroIOError.super_.call(this, this.constructor, arguments);
};   
util.inherits(AvroIOError, AbstractAvroError);
AvroIOError.prototype.message = 'Avro IO Error';

// Error objects
var AvroFileError = function() { 
    AvroFileError.super_.call(this, this.constructor, arguments);
};   
util.inherits(AvroFileError, AbstractAvroError);
AvroFileError.prototype.message = 'Avro File Error';
    
var AvroBlockError = function() {
    AvroBlockError.super_.call(this, this.constructor, arguments);
};
util.inherits(AvroBlockError, AbstractAvroError);
AvroBlockError.prototype.message = 'Avro Block Error';

var AvroBlockDelayReadError = function() {
    AvroBlockDelayReadError.super_.call(this, this.constructor, arguments);
};
util.inherits(AvroBlockDelayReadError, AbstractAvroError);
AvroBlockDelayReadError.prototype.message = 'Avro Block Delay Read Error';

module.exports = {
    IOError: AvroIOError,
    FileError: AvroFileError,
    BlockError: AvroBlockError,
    BlockDelayReadError: AvroBlockDelayReadError
}