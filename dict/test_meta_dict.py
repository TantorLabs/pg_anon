{
	"field": {				# must be anonymized without scanning
		"rules": [
			"^fld_5_em",
			"^amount",
			"details$",
			"contract_expires$",
			"inn$"
		],
		"constants": [
			"usd",
			"имя_поля"
		]
	},
	"skip_rules": [
		{
			"schema": "schm_mask_ext_exclude_2",
			"table": "card_numbers",    # Optional. If no "table" then whole schema will be skipped
			"fields": ["val_skip"]		# Optional. If no "fields" then whole table will be skipped
		}
	],
	"data_regex": {
		"rules": [
			"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",  # email
			"7?[\d]{10}",				# phone 7XXXXXXXXXX
			"^other_ext_tbl_text",		# catch "schm_mask_ext_exclude_2.other_ext_tbl_2"
			"""[0-9]{3}-[0-9]{2}-[0-9]{4}""",  # social Security numbers "nnn-nn-nnnn"
			"""\b[0-9A-Z]{3}([^ 0-9A-Z]|\s)?[0-9]{4}\b""",	# license plate numbers aaa-nnnn
			"""^\d{1,3}[.]\d{1,3}[.]\d{1,3}[.]\d{1,3}$""",	# IPV4 addresses
			"""^([1][12]|[0]?[1-9])[\/-]([3][01]|[12]\d|[0]?[1-9])[\/-](\d{4}|\d{2})$""",  # Dates in MM/DD/YYYY format
			# MasterCard numbers 5258704108753590
			"""^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$""",
			# Visa card numbers 4563-7568-5698-4587
			"""\b([4]\d{3}[\s]\d{4}[\s]\d{4}[\s]\d{4}|[4]\d{3}[-]\d{4}[-]\d{4}[-]\d{4}|[4]\d{3}[.]\d{4}[.]\d{4}[.]\d{4}|[4]\d{3}\d{4}\d{4}\d{4})\b""",
			# Any card number
			"""[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}""",
			# URLs
			"""(?i)\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()]+|\(([^\s()]+|(\([^\s()]+\)))*\))+(?:\(([^\s()]+|(\([^\s()]+\)))*\)|[^\s`!()\[\]{};:'".,?«»“”‘’]))""",
			"""[0-9]{2}-[0-9]{7}"""  # INN from 1c
		]
	},
	"data_const": {
		"constants": [
			"account",
			"email",
			"слово",
			"сергей"
		]
	},
	"sens_pg_types": [
		"text",
		"integer",
		"bigint",
		"character",
		"json"
	],
	"funcs": {
		"text": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')",
		"numeric": "anon_funcs.noise(\"%s\", 10)",
		"timestamp": "anon_funcs.dnoise(\"%s\",  interval '6 month')",
		"bigint": "anon_funcs.random_inn()",
		"integer": "anon_funcs.random_int_between(1, 10)",
		"mvarchar": "anon_funcs.digest(\"%s\"::text, 'salt_word', 'md5')"
	}
}