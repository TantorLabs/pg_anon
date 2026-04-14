{
	"field": {
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
			"table": "card_numbers",
			"fields": ["val_skip"]
		},
		{
			"schema": "schm_other_3",
		},
	],
	"data_regex": {
		"rules": [
			r"""[A-Za-z0-9]+([._-][A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+""",
			r"^(7?\d{10})$",
			r"^other_ext_tbl_text",
			r"""[0-9]{3}-[0-9]{2}-[0-9]{4}""",
			r"""\b[0-9A-Z]{3}([^ 0-9A-Z]|\s)?[0-9]{4}\b""",
			r"""^\d{1,3}[.]\d{1,3}[.]\d{1,3}[.]\d{1,3}$""",
			r"""^([1][12]|[0]?[1-9])[\/-]([3][01]|[12]\d|[0]?[1-9])[\/-](\d{4}|\d{2})$""",
			r"""^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$""",
			r"""\b([4]\d{3}[\s]\d{4}[\s]\d{4}[\s]\d{4}|[4]\d{3}[-]\d{4}[-]\d{4}[-]\d{4}|[4]\d{3}[.]\d{4}[.]\d{4}[.]\d{4}|[4]\d{3}\d{4}\d{4}\d{4})\b""",
			r"""[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}""",
			r"""(?i)\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()]+|\(([^\s()]+|(\([^\s()]+\)))*\))+(?:\(([^\s()]+|(\([^\s()]+\)))*\)|[^\s`!()\[\]{};:’".,?«»""’’]))""",
			r"""[0-9]{2}-[0-9]{7}"""
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
		"varchar",
		"json"
	],
	"funcs": {
		"text": "anon_funcs.digest(\"%s\", 'salt_word', 'md5')",
		"numeric": "anon_funcs.noise(\"%s\", 10)",
		"numeric(30,4)": "anon_funcs.noise(\"%s\", 30)",
		"timestamp": "anon_funcs.dnoise(\"%s\",  interval '6 month')",
		"bigint": "LPAD((10000000 + ROW_NUMBER() OVER (ORDER BY inn))::TEXT, 8, '0')",
		"integer": "anon_funcs.random_int_between(1, 10)",
		"mvarchar": "anon_funcs.digest(\"%s\"::text, 'salt_word', 'md5')"
	}
}
