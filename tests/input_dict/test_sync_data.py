{
	"dictionary": [
		{
			"schema":"schm_other_2",
			"table":"exclude_tbl",
			"fields": {
					"val":"'text const modified'"
			}
		},
		{
			"schema":"schm_other_2",
			"table":"some_tbl",
			"raw_sql": "SELECT id, val || ' modified 2' as val FROM schm_other_2.some_tbl"
		},
		{
			"schema":"schm_mask_include_1",
			"table":"tbl_123",
			"fields": {
					"val":"anon_funcs.partial(val,1,'***',3)"
			}
		}
    ],
	"dictionary_exclude": [
		{
			"schema_mask": "*",
			"table_mask": "*",
		}
	]
}