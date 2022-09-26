{
	"field": {
		"rules": [
			"^fld_5_em",
			"^amount"
		]
	},
	"data_regex": {
		"rules": [
			"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+",  # email
			"7?[\d]{10}"	# phone 7XXXXXXXXXX
		]
	},
	"data_const": {
		"constants": [
			"bank",
			"account",
			"email"
		]
	}
}