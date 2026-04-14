{
	"dictionary": [
		{
			"schema":"schm_other_1",
			"table":"some_tbl",
			"fields": {
					"val":"'text const modified'"
			}
		},
		{
			"schema":"schm_other_2",
			"table":"some_tbl",
			"raw_sql": "SELECT id, val || ' modified 2' as val FROM schm_other_2.some_tbl"
		}
    ],
	"dictionary_exclude": [
		{
			"schema_mask": "*",
			"table_mask": "*",
		}
	]
}