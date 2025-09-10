{
	"field": {
		"rules": [
			"^field_type_",
		],
	},
	"sens_pg_types": [
		"varbit"
		"bool"
		"char"
		"varchar"
		"int"
		"int4"
		"int2"
		"int8"
		"float"
		"float8"
		"float4"
		"decimal"
		"serial2"
		"serial4"
		"serial8"
		"time"
		"timetz"
		"timestamp"
		"timestamptz"
	],
	"funcs": {
		"varbit": "anon_funcs.digest(\"%s\", 'varbit', 'md5')",
		"bool": "anon_funcs.digest(\"%s\", 'bool', 'md5')",
		"char": "anon_funcs.digest(\"%s\", 'char', 'md5')",
		"varchar": "anon_funcs.digest(\"%s\", 'varchar', 'md5')",
		"int": "anon_funcs.digest(\"%s\", 'int', 'md5')",
		"int4": "anon_funcs.digest(\"%s\", 'int4', 'md5')",
		"int2": "anon_funcs.digest(\"%s\", 'int2', 'md5')",
		"int8": "anon_funcs.digest(\"%s\", 'int8', 'md5')",
		"float": "anon_funcs.digest(\"%s\", 'float', 'md5')",
		"float8": "anon_funcs.digest(\"%s\", 'float8', 'md5')",
		"float4": "anon_funcs.digest(\"%s\", 'float4', 'md5')",
		"decimal": "anon_funcs.digest(\"%s\", 'decimal', 'md5')",
		"serial2": "anon_funcs.digest(\"%s\", 'serial2', 'md5')",
		"serial4": "anon_funcs.digest(\"%s\", 'serial4', 'md5')",
		"serial8": "anon_funcs.digest(\"%s\", 'serial8', 'md5')",
		"time": "anon_funcs.digest(\"%s\", 'time', 'md5')",
		"timetz": "anon_funcs.digest(\"%s\", 'timetz', 'md5')",
		"timestamp": "anon_funcs.digest(\"%s\", 'timestamp', 'md5')",
		"timestamptz": "anon_funcs.digest(\"%s\", 'timestamptz', 'md5')",
	}
}
