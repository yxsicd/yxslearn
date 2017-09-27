function ArrayNode() {
    var that = this;
    that.value = [];
    that.get = function get(i) {
        return that.value[i];
    }

    that.set = function set(i, v) {
        that.value[i] = v;
    }

    that.size = function() {
        return that.value.length;
    }

    return that;
}

var an = new ArrayNode();
an.set(3, "abc")
an.get(3)

function jsArray() {
    var that = this;
    that.value = new ArrayNode();
    that.value.set(0, "v0")
    that.value.set(1, "v1")
    that.value.get(1)

    that.forEach = function(callback) {
        for (var i = 0; i < that.value.size(); i++) {
            var item = that.value.get(i);
            var index = i;
            callback ? callback(item, index) : 0;
        }
    }
    return that;
}

var js = new jsArray();
js.forEach(function(v, i) {
    console.log(v, i);
})
