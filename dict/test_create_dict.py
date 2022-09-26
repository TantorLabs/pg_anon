{
	"gen_dictionary": [
		{
			"obj_type": "field"
			"rules": [
				"^fld_name"
			]
		},
		{
			"obj_type": "data"
			"rules": [
				r'^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$'			# https://regexr.com/3e48o
			]
		},
		{
			"obj_type": "data"
			"constants": [
				"word1",
				"word2"
			]
		}
	]
}