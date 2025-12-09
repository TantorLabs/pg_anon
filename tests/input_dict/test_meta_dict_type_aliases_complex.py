{
	"include_rules": [
		{
			"schema": "schm_other_3",
			"table": "data_types_test",
		}
	],
	"field": {
		"rules": [".*"]
	},
	"funcs": {
        "default": "anon_funcs.digest(\"%s\", 'default', 'md5')",
        "character varying  	(20)": "anon_funcs.digest(\"%s\", 'varchar(20)', 'md5')",
		"bit varying     		(5)  ": "anon_funcs.digest(\"%s\", 'varbit(5)', 'md5')",
		"time        	 		(3)     without time zone": "anon_funcs.digest(\"%s\", 'time(3)', 'md5')",
		"time        	 		(3) with time zone": "anon_funcs.digest(\"%s\", 'timetz(3)', 'md5')",
		"double precision": "anon_funcs.digest(\"%s\", 'float', 'md5')",
    }
}
