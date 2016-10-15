
function checkData(id, data, checker) {
	var onecheck = checker[id];
	var ret = {};

	for ( var key in data) {
		var value = data[key];
		if (onecheck[key]) {
			if (onecheck[key]["pattern"]
					&& !value.match(onecheck[key]["pattern"])) {
				ret.resid = id;
				ret.pid = key;
				ret.ruletype = "pattern";
				ret.value = value;
				ret.rulevalue = onecheck[key]["pattern"]
				return ret;
			}
			if (onecheck[key]["type"]) {
				var type = onecheck[key]["type"];
				if (type == "number") {
					if (isNaN(value)) {
						ret.resid = id;
						ret.pid = key;
						ret.ruletype = "type";
						ret.value = value;
						ret.rulevalue = type;
						return ret;
					}
				}
			}
			if (onecheck[key]["range"]) {
				var range = onecheck[key]["range"];
				if (value < range[0] || value > range[1]) {
					ret.resid = id;
					ret.pid = key;
					ret.ruletype = "range";
					ret.value = value;
					ret.rulevalue = onecheck[key]["range"];
					return ret;
				}
			}
			if (onecheck[key]["checkfunction"]) {
				var range = onecheck[key]["checkfunction"];
				if (!range(value)) {
					ret.resid = id;
					ret.pid = key;
					ret.ruletype = "checkfunction";
					ret.value = value;
					ret.rulevalue = onecheck[key]["checkfunction"].toString();
					return ret;
				}
			}
			
		}
	}
}

function main() {
	var data = JSON.parse(checkdata);
	var retlist=[];
	for(var i=0;i<data.length;i++)
	{
		var ret= checkData(1010, data[i], checker);
		retlist.push(ret);
	}
	
	return JSON.stringify(retlist);
}

main();
