var AE = require('./lib/errors.js');

try {

    throw new AE.BlockError('Got a block error at %d', 2422);

}
catch(e) {
    console.error(e.__proto__);
    console.error(typeof(e));
    console.error(e.message);
    console.error(e.stack);
}

console.log('got here');
