var checker = {
	"1010" : {
		"1001" : {
			"type" : "string",
			"pattern" : "",
			"range" : [ 5, 10 ],
			"checkfunction":function(data){
				return data&&data.indexOf("7")!=-1;
			}
		},
		"1002" : {
			"type" : "number",
			"pattern" : "",
			"range" : [ 3, 7 ]
		},
		"1003" : {
			"type" : "number",
			"pattern" : "",
			"range" : [ 5, 8 ]
		}
	}
};

