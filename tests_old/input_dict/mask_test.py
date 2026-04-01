{
	"dictionary": [
		{
			"schema_mask": "*",
			"table_mask": "*",
			"fields": {
				"amount": "101010"
			}
		},
		{
			"schema":"schm_other_1",
			"table":"some_tbl",
			"fields": {
					"val":"'text const'"
			}
		},
		{
			"schema_mask": "*",
			"table": "tbl_100",
			"fields": {
				"amount": "202020"
			}
		},
		{
			"schema":"schm_other_2",
			"table":"some_tbl",
			"raw_sql": "SELECT id, val || ' modified' as val FROM schm_other_2.some_tbl"
		}
	]
}