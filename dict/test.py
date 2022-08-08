{
	"dictionary": [
		{
			"schema":"schm_other_1",
			"table":"some_tbl",
			"fields": {
					"val":"'text const'"
			}
		},
		{
			"schema":"schm_other_2",
			"table":"some_tbl",
			"raw_sql": "SELECT id, val || ' modified' as val FROM schm_other_2.some_tbl"
		},
		{
			"schema":"public",
			"table":"key_value",
			"fields": {
					"fld_value":"""SQL:
					CASE
						WHEN "fld_key" ILIKE '%email%' THEN CONCAT(md5(random()::TEXT),'@domain.com')
						WHEN "fld_key" ILIKE '%password%' THEN md5(fld_value)
						WHEN "fld_key" ILIKE '%address%' THEN 'test address'
						WHEN "fld_key" ILIKE '%login%' THEN 'test_login'
						WHEN "fld_key" ILIKE '%name%' THEN 'test_name'
						WHEN "fld_key" ILIKE '%amount%' THEN (select anon_funcs.noise(fld_value::int, 1000.2)::text)
						ELSE fld_value
					END"""
			}
		},
		{
			"schema":"_SCHM.$complex#имя;@&* a'",
			"table":"_TBL.$complex#имя;@&* a'2",
			"fields": {
				"_FLD.$complex#имя;@&* a'": "'text const'"
			}
		},
		{
			"schema":"_SCHM.$complex#имя;@&* a'",
			"table":"_TBL.$complex#имя;@&* a'3",
			"raw_sql": """
				SELECT id, fld_key, "_FLD.$complex#имя;@&* a'" || ' (modified)' as "_FLD.$complex#имя;@&* a'"
				FROM "_SCHM.$complex#имя;@&* a'"."_TBL.$complex#имя;@&* a'3"
			"""
		},
		{
			"schema":"schm_other_2",
			"table":"tbl_test_anon_functions",
			"fields": {
					"fld_1_int": "anon_funcs.noise(fld_1_int, 2000)",
					"fld_2_datetime": "anon_funcs.dnoise(fld_2_datetime, interval '1 month')",
					"fld_3_txt": "anon_funcs.digest(fld_3_txt, 'salt', 'sha256') ",
					"fld_4_txt": "anon_funcs.partial(fld_4_txt,1,'***',3)",
					"fld_5_email": "anon_funcs.partial_email(fld_5_email)",
					"fld_6_txt": "anon_funcs.random_string(7)",
					"fld_7_zip": "anon_funcs.random_zip()",
					"fld_8_datetime": """
						anon_funcs.random_date_between(
							fld_8_datetime - interval '1 year',
							fld_8_datetime + interval '1 year'
						)
					""",
					"fld_9_datetime": "anon_funcs.random_date()",
					"fld_10_int": "anon_funcs.random_int_between(fld_10_int - 1000, fld_10_int + 2000)",
					"fld_11_int": "anon_funcs.random_bigint_between(6000000000, 7000000000)",
					"fld_12_phone": "anon_funcs.random_phone('+7')",
					"fld_13_txt": "anon_funcs.random_hash('seed', 'sha512')",
					"fld_14_txt": "anon_funcs.random_in(array['a', 'b', 'c'])",
					"fld_15_txt": "anon_funcs.hex_to_int(fld_15_txt)::text"
			}
		}
	],
	"dictionary_exclude": [
		{
			"schema":"schm_other_2",
			"table":"exclude_tbl"
		}
	]
}