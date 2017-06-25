var git = require('simple-git')("gittest/yl");
// simpleGit.clone("https://yxsicd:kelayin1@github.com/yxsicd/yxslearn.git", "yl", null, function (err, data) {
//   console.log(arguments);
// });




git.listRemote(['--heads', '--tags'], function (err, data) {
  var lines = data.split(/\n/g).filter(function (d) { return !d.match(/^$/) });
  var allref = lines.map(function (d, i) {
    var arr = d.split("\t");
    var commitid = arr[0];
    var refer = arr[1];
    var branch = refer.split("/")[2];
    return { refer: refer, commitid: commitid, branch: branch };
  });
  console.log(allref);

  allref.forEach(function (d, i) {
    git.show([`${d["commitid"]}:tt.md`], function (err, data) {
      console.log(arguments);
    });

    git.revparse([`${d["commitid"]}`], function (err, data) { 
      console.log(arguments);
    });

  });

});

