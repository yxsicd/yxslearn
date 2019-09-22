function getmid(a, b) {
    var mid = (a + b) / 2
    for (var i = 0; i < 100; i++) {
        var value = Math.pow(10, i);
        var testMid = Math.round(mid * value) / value
        if (testMid > a && testMid < b) {
            return testMid
        }
    }
    return mid;
}
var chars = [];
var charsMap = {};
for (var i = 0; i < 255; i++) {

    var code_0 = "0".charCodeAt(0);
    var code_9 = "9".charCodeAt(0);
    var code_a = "a".charCodeAt(0);
    var code_z = "z".charCodeAt(0);
    if ((i >= code_0 && i <= code_9) || (i >= code_a && i <= code_z)) {
        var index = chars.length;
        var nowChar = String.fromCharCode(i);
        chars.push(nowChar);
        charsMap[nowChar] = index;
    }

}
console.log(chars, charsMap)

function getmidstring(a, b) {

    var ret = [];
    for (var i = 0; i < 65535; i++) {
        var a_i = a.charAt(i);
        var b_i = b.charAt(i);
        a_i=a_i?a_i:"0"
        b_i=b_i?b_i:"z"

        if (a_i == b_i) {
            ret.push(a_i);
            continue;
        } else {
            var a_index = charsMap[a_i];
            var b_index = charsMap[b_i];
            var index_mid = Math.round((a_index + b_index) / 2)
            if (index_mid > a_index && index_mid < b_index) {
                ret.push(chars[index_mid])
                break;
            } else {
                ret.push(a_i);
                continue;
            }
        }
    }
    return ret.join("");
}

var a = '0'
var b = 'z'

var a_0 = a;
var b_0 = b;

for (var i = 0; i < 60000; i++) {
    var oldb = b;
    var olda = a;
    b = getmidstring(a_0, oldb)
    a = getmidstring(olda, b_0)
    console.log(a_0, b+"", oldb+"", i+"", b.length)
    console.log(olda+"", a+"", b_0, i+"", a.length)
}
