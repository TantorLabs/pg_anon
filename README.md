# README #


### Installation ###
```python
apt install -y python3-pip
pip3 install -r requirements.txt
python3 test/full_test.py -v
>>
	Ran N tests in ...
	OK

# If all tests is OK then application ready to use

```


### How to escape/unescape complex names of objects ###

```python
python3

import json
j = {"k": "_TBL.$complex#имя;@&* a'2"}
json.dumps(j)
>>
	'{"k": "_TBL.$complex#\\u0438\\u043c\\u044f;@&* a\'2"}'

s = '{"k": "_TBL.$complex#\\u0438\\u043c\\u044f;@&* a\'2"}'
u = json.loads(s)
print(u['k'])
>>
	_TBL.$complex#имя;@&* a'2

```

