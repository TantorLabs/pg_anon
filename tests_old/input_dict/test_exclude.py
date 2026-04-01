{
	"dictionary": [
		{
			"schema":"schm_other_1",
			"table":"some_tbl",
			"fields": {
					"val":"'text const'"
			}
		}
	],
	"dictionary_exclude": [
		{
			"schema_mask": "*",
			"table_mask": "*",
		}
	],
	"validate_tables": [		# only this tables must contains rows
		{
			"schema": "schm_other_1",
			"table": "some_tbl"
		}
	]
}