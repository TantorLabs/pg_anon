# 📚 SQL Functions Library

> [🏠 Home](../README.md#-documentation-index) | [🏗️ Init](operations/init.md) | [🔍 Scan](operations/scan.md) | [💾 Dump](operations/dump.md) | [🔬 View Fields](operations/view-fields.md) | [📊 View Data](operations/view-data.md) | [🗂️ Meta Dictionary](dicts/meta-dict-schema.md) | [🔐 Sensitive Dictionary](dicts/sens-dict-schema.md)  

## Overview

All functions are contained in the `init.sql` file. After run pg_anon in `init` mode, they will reside in the `anon_funcs` schema in the source database.
If you want to write a new function, simply create it in the `anon_funcs` schema in your source database.

List of some functions available for use in dictionaries:

---

## Functions list

### 1. noise
Add noise to a real number:
```SQL
SELECT anon_funcs.noise(100, 1.2);
>> 123
```

### 2. dnoise
Add noise to a date or timestamp:
```SQL
SELECT anon_funcs.dnoise('2020-02-02 10:10:10'::timestamp, interval '1 month');
>> 2020-03-02 10:10:10
```

### 3. digest
Hash a string value with a specified hash function:
```SQL
SELECT anon_funcs.digest('text', 'salt', 'sha256');
>> '3353e....'
```

### 4. partial
Keep the first few characters (2nd argument) and the last few characters (4th argument) of the specified string, adding a constant (3rd argument) in between:
```SQL
SELECT anon_funcs.partial('123456789', 1, '***', 3);
>> 1***789
```

### 5. partial_email
Mask an email address:
```SQL
SELECT anon_funcs.partial_email('example@gmail.com');
>> ex*****@gm*****.com
```

### 6. random_string
Generate a random string of specified length:
```SQL
SELECT anon_funcs.random_string(7);
>> H3ZVL5P
```

### 7. random_zip
Generate a random ZIP code:
```SQL
SELECT anon_funcs.random_zip();
>> 851467
```

### 8. random_date_between
Generate a random date and time within a specified range:
```SQL
SELECT anon_funcs.random_date_between(
   '2020-02-02 10:10:10'::timestamp,
   '2022-02-05 10:10:10'::timestamp
);
>> 2021-11-08 06:47:48.057
```

### 9. random_date
Generate a random date and time:
```SQL
SELECT anon_funcs.random_date();
>> 1911-04-18 21:54:13.139
```

### 10. random_int_between
Generate a random integer within a specified range:
```SQL
SELECT anon_funcs.random_int_between(100, 200);
>> 159
```

### 11. random_bigint_between
Generate a random bigint within a specified range:
```SQL
SELECT anon_funcs.random_bigint_between(6000000000, 7000000000);
>> 6268278565
```

### 12. random_phone
Generate a random phone number:
```SQL
SELECT anon_funcs.random_phone('+7');
>> +7297479867
```

### 13. random_hash
Generate a random hash using the specified function:
```SQL
SELECT anon_funcs.random_hash('seed', 'sha512');
>> b972f895ebea9cf2f65e19abc151b8031926c4a332471dc5c40fab608950870d6dbddcd18c7e467563f9b527e63d4d13870e4961c0ff2a62f021827654ae51fd
```

### 14. random_in
Select a random element from an array:
```SQL
SELECT anon_funcs.random_in(array['a', 'b', 'c']);
>> a
```

### 15. hex_to_int
Convert a hexadecimal value to decimal:
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
Also, adding new anonymization functions can be performed by adding `init.sql` to the file and then run pg_anon in `init` mode.
