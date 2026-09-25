# 📚 SQL Functions Library

> [🏠 Home](../README.md#-documentation-index) | [🏗️ Init](operations/init.md) | [🔍 Scan](operations/scan.md) | [🗂️ Meta Dictionary](dicts/meta-dict-schema.md) | [🔐 Sensitive Dictionary](dicts/sens-dict-schema.md)

## Overview

All functions are contained in the [init.sql](../pg_anon/init.sql) file. After run pg_anon in `init` mode, they will reside in the `anon_funcs` schema in the source database.
If you want to write a new function, simply create it in the `anon_funcs` schema in your source database.

List of some functions available for use in dictionaries:

---

## Functions list

### 1. noise
Add random noise to a numeric value by scaling it with a random factor within `±ratio`.

**Signature:**
```SQL
anon_funcs.noise(noise_value ANYELEMENT, ratio DOUBLE PRECISION) RETURNS ANYELEMENT
```
| Argument      | Type               | Description                                  |
|---------------|--------------------|----------------------------------------------|
| `noise_value` | `ANYELEMENT`       | Numeric value to add noise to                |
| `ratio`       | `DOUBLE PRECISION` | Maximum relative deviation (e.g. `0.2` = ±20%) |

**Returns:** `ANYELEMENT` — the same numeric type as the input value. The magnitude stays close to the original (within the given ratio); precision is that of the input type.

**Example:**
```SQL
SELECT anon_funcs.noise(100, 1.2);
>> 123
```

### 2. dnoise
Add random noise to a date or timestamp by shifting it by a random interval within `±noise_range`.

**Signature:**
```SQL
anon_funcs.dnoise(noise_value ANYELEMENT, noise_range INTERVAL) RETURNS ANYELEMENT
```
| Argument      | Type         | Description                                |
|---------------|--------------|--------------------------------------------|
| `noise_value` | `ANYELEMENT` | Date/time value to add noise to            |
| `noise_range` | `INTERVAL`   | Maximum shift in either direction          |

**Returns:** `ANYELEMENT` — the same date/time type as the input value; precision is unchanged.

**Example:**
```SQL
SELECT anon_funcs.dnoise('2020-02-02 10:10:10'::timestamp, interval '1 month');
>> 2020-03-02 10:10:10
```

### 3. digest
Hash a string value with a specified hash algorithm (`seed` + `salt`).

**Signature:**
```SQL
anon_funcs.digest(seed TEXT, salt TEXT, algorithm TEXT) RETURNS TEXT
```
| Argument    | Type   | Description                                                     |
|-------------|--------|-----------------------------------------------------------------|
| `seed`      | `TEXT` | Input string to hash                                            |
| `salt`      | `TEXT` | Salt appended to the seed before hashing                        |
| `algorithm` | `TEXT` | Hash algorithm: `md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512` |

**Returns:** `TEXT` — a hex-encoded hash. Length depends on the algorithm: `md5` → 32, `sha1` → 40, `sha224` → 56, `sha256` → 64, `sha384` → 96, `sha512` → 128 hex characters.

**Example:**
```SQL
SELECT anon_funcs.digest('text', 'salt', 'sha256');
>> '3353e....'
```

### 4. partial
Keep the first `prefix` characters and the last `suffix` characters of the string, inserting the `padding` constant in between.

**Signature:**
```SQL
anon_funcs.partial(ov TEXT, prefix INT, padding TEXT, suffix INT) RETURNS TEXT
```
| Argument  | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| `ov`      | `TEXT` | Original value to mask                               |
| `prefix`  | `INT`  | Number of leading characters to keep                 |
| `padding` | `TEXT` | Constant inserted between the kept prefix and suffix |
| `suffix`  | `INT`  | Number of trailing characters to keep                |

**Returns:** `TEXT`. Length = `prefix` + length(`padding`) + `suffix` characters.

**Example:**
```SQL
SELECT anon_funcs.partial('123456789', 2, '***', 3);
>> 12***789
```

### 5. partial_email
Mask an email address.

**Signature:**
```SQL
anon_funcs.partial_email(ov TEXT) RETURNS TEXT
```
| Argument | Type   | Description                       |
|----------|--------|-----------------------------------|
| `ov`     | `TEXT` | Original email address to mask    |

**Returns:** `TEXT` — a masked email of variable length: first 2 characters of the local part + `******` + `@` + first characters of the domain + `******` + original top-level domain (e.g. `.com`).

**Example:**
```SQL
SELECT anon_funcs.partial_email('example@gmail.com');
>> ex******@gm******.com
```

### 6. random_string
Generate a random string of the specified length using characters `A–Z` and `0–9`.

**Signature:**
```SQL
anon_funcs.random_string(l INTEGER) RETURNS TEXT
```
| Argument | Type      | Description                      |
|----------|-----------|----------------------------------|
| `l`      | `INTEGER` | Length of the generated string   |

**Returns:** `TEXT` of exactly `l` characters.

**Example:**
```SQL
SELECT anon_funcs.random_string(7);
>> H3ZVL5P
```

### 7. random_zip
Generate a random ZIP code.

**Signature:**
```SQL
anon_funcs.random_zip() RETURNS TEXT
```
**Returns:** `TEXT` — exactly 6 digits.

**Example:**
```SQL
SELECT anon_funcs.random_zip();
>> 851467
```

### 8. random_inn
Generate a random numeric identifier (e.g. an INN-like value).

**Signature:**
```SQL
anon_funcs.random_inn() RETURNS TEXT
```

**Returns:** `TEXT` — exactly 8 digits.

**Example:**
```SQL
SELECT anon_funcs.random_inn();
>> 50734812
```

### 9. random_date_between
Generate a random date and time within the `[date_start, date_end]` range.

**Signature:**
```SQL
anon_funcs.random_date_between(date_start TIMESTAMPTZ, date_end TIMESTAMPTZ) RETURNS TIMESTAMPTZ
```
| Argument     | Type          | Description                        |
|--------------|---------------|------------------------------------|
| `date_start` | `TIMESTAMPTZ` | Lower bound of the range (inclusive) |
| `date_end`   | `TIMESTAMPTZ` | Upper bound of the range (inclusive) |

**Returns:** `timestamp with time zone` within the given range.

**Example:**
```SQL
SELECT anon_funcs.random_date_between(
   '2020-02-02 10:10:10'::timestamp,
   '2022-02-05 10:10:10'::timestamp
);
>> 2021-11-08 06:47:48.057
```

### 10. random_date
Generate a random date and time between `1900-01-01` and the current moment.

**Signature:**
```SQL
anon_funcs.random_date() RETURNS TIMESTAMPTZ
```

**Returns:** `timestamp with time zone`.

**Example:**
```SQL
SELECT anon_funcs.random_date();
>> 1911-04-18 21:54:13.139
```

### 11. random_int_between
Generate a random integer within the specified range.

**Signature:**
```SQL
anon_funcs.random_int_between(int_start INTEGER, int_stop INTEGER) RETURNS INTEGER
```
| Argument    | Type      | Description              |
|-------------|-----------|--------------------------|
| `int_start` | `INTEGER` | Lower bound (inclusive)  |
| `int_stop`  | `INTEGER` | Upper bound (exclusive)  |

**Returns:** `INTEGER` in the range `[int_start, int_stop)`.

**Example:**
```SQL
SELECT anon_funcs.random_int_between(100, 200);
>> 159
```

### 12. random_bigint_between
Generate a random bigint within the specified range.

**Signature:**
```SQL
anon_funcs.random_bigint_between(int_start BIGINT, int_stop BIGINT) RETURNS BIGINT
```
| Argument    | Type     | Description              |
|-------------|----------|--------------------------|
| `int_start` | `BIGINT` | Lower bound (inclusive)  |
| `int_stop`  | `BIGINT` | Upper bound (exclusive)  |

**Returns:** `BIGINT` in the range `[int_start, int_stop)`.

**Example:**
```SQL
SELECT anon_funcs.random_bigint_between(6000000000, 7000000000);
>> 6268278565
```

### 13. random_phone
Generate a random phone number with the given prefix (defaults to `0`).

**Signature:**
```SQL
anon_funcs.random_phone(phone_prefix TEXT DEFAULT '0') RETURNS TEXT
```
| Argument       | Type   | Description                                                |
|----------------|--------|------------------------------------------------------------|
| `phone_prefix` | `TEXT` | Prefix prepended to a random 9-digit number (default `'0'`) |

**Returns:** `TEXT` — `phone_prefix` followed by a random 9-digit number. Length = length(`phone_prefix`) + 9.

**Example:**
```SQL
SELECT anon_funcs.random_phone('+7');
>> +7297479867
```

### 14. random_hash
Generate a hash of `seed` with the specified algorithm, using a random 6-character salt (so the result differs between calls).

**Signature:**
```SQL
anon_funcs.random_hash(seed TEXT, algorithm TEXT) RETURNS TEXT
```
| Argument    | Type   | Description                                                     |
|-------------|--------|-----------------------------------------------------------------|
| `seed`      | `TEXT` | Input string to hash                                            |
| `algorithm` | `TEXT` | Hash algorithm: `md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512` |

**Returns:** `TEXT` — a hex-encoded hash. Length depends on the algorithm (same as `digest`): `md5` → 32, `sha1` → 40, `sha224` → 56, `sha256` → 64, `sha384` → 96, `sha512` → 128 hex characters.

**Example:**
```SQL
SELECT anon_funcs.random_hash('seed', 'sha512');
>> b972f895ebea9cf2f65e19abc151b8031926c4a332471dc5c40fab608950870d6dbddcd18c7e467563f9b527e63d4d13870e4961c0ff2a62f021827654ae51fd
```

### 15. random_in
Select a random element from an array.

**Signature:**
```SQL
anon_funcs.random_in(a ANYARRAY) RETURNS ANYELEMENT
```
| Argument | Type       | Description                          |
|----------|------------|--------------------------------------|
| `a`      | `ANYARRAY` | Array to pick a random element from  |

**Returns:** `ANYELEMENT` — one element of the input array; type and length match the array element.

**Example:**
```SQL
SELECT anon_funcs.random_in(array['a', 'b', 'c']);
>> a
```

### 16. hex_to_int
Convert a hexadecimal value to decimal.

**Signature:**
```SQL
anon_funcs.hex_to_int(hexval TEXT) RETURNS INT
```
| Argument | Type   | Description                    |
|----------|--------|--------------------------------|
| `hexval` | `TEXT` | Hexadecimal string to convert  |

**Returns:** `INT` — the decimal value (must fit into a 4-byte `INT`).

**Example:**
```SQL
SELECT anon_funcs.hex_to_int('8AB');
>> 2219
```

---

## pgcrypto
In addition to the existing functions in the anon_funcs schema, functions from the pgcrypto extension can also be used.
```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

Example of using encryption with base64 encoding to store the encrypted value in a text field:
```SQL
SELECT encode((SELECT encrypt('data', 'password', 'bf')), 'base64');
>> cSMq9gb1vOw=

SELECT decrypt(
(
SELECT decode('cSMq9gb1vOw=', 'base64')
), 'password', 'bf');
>> data
```

---

## How to add your own functions
Also, adding new masking functions can be performed by adding [init.sql](../pg_anon/init.sql) to the file and then run pg_anon in `init` mode.
