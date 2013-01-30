var AE = require('./lib/errors.js');

try {

    throw new AE.FileError('Got a block error at %d', 2422);

}
catch(e) {
    console.error("e instanceof AE.BlockError %s", e instanceof AE.BlockError);
    console.error("e instanceof Error %s", e instanceof Error);
    throw e;
}

console.log('got here');
