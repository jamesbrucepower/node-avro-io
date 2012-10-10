function Test() {

    this.aFunction = function() {};
    this.aVariable = "me";
}

Test.prototype = {
    
    extendedF: function() {}
}

module.exports = Test;

