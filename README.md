# 🔒 pg_anon

**Anonymization tool for PostgreSQL.**  
Share real data without exposing personal or confidential information.

---

## ✨ Overview

`pg_anon` helps you safely clone your production database for testing or development.  
During the process, all sensitive fields are replaced with realistic but fake values —  
keeping your data structure intact and your privacy protected.

---

## ⚙️ Requirements

- **Python:** 3.11+
- **PostgreSQL:** 9.6+
- **PostgreSQL client utilities (must match the server’s major version):**
  - `pg_dump` – uses for export the database schema  
  - `pg_restore` – uses for restore the database schema into the target database

For details, see: [Installation and configuring](docs/installation-and-configuring.md#configuring-pg_anon)

---

## 🧩 Terminology

| Term                                  | Description                                                                                                                                                                                             |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Personal (sensitive) data**         | Data that must not be shared with third parties. Includes personal or confidential business information.                                                                                                |
| **Source database**                   | The original database that contains sensitive data.                                                                                                                                                     |
| **Target database**                   | An empty database where anonymized data will be restored.                                                                                                                                               |
| **Meta-dictionary**                   | A Python file describing rules for detecting sensitive data. Created manually and used as the basis for generating the sensitive dictionary during scanning. [See more](docs/dicts/meta-dict-schema.md) |
| **Prepared sensitive dictionary**     | A Python file that defines which tables and fields contain sensitive data and how to anonymize them. Created automatically or manually. [See more](docs/dicts/sens-dict-schema.md)                      |
| **Prepared non-sensitive dictionary** | A Python file listing schemas, tables, and fields without sensitive data. Used to speed up repeated scans. [See more](docs/dicts/non-sens-dict-schema.md)                                               |
| **Partial dump & restore dictionary** | A Python file listing tables. Used to include or exclude tables from dump & restore operations. [See more](docs/dicts/tables-dictionary.md)                                                                |
| **Create-dict (scan)**                | The process of scanning the source database to detect sensitive fields and create dictionary files. [See more](docs/operations/scan.md)                                                                 |
| **Dump**                              | Exporting data from the source database into files using a dictionary. This is where anonymization occurs. [See more](docs/operations/dump.md)                                                          |
| **Restore**                           | Importing anonymized data from files into the target database. [See more](docs/operations/restore.md)                                                                                                   |
| **Anonymization (masking)**           | Full process of cloning and sanitizing data (`dump → restore`), replacing sensitive values with random or hashed ones.                                                                                  |
| **Anonymization function**            | A PostgreSQL function (built-in or from `anon_funcs` schema) that replaces sensitive values with random or hashed data. New functions can be added to extend anonymization logic.                       |

## 🚀 Quick Start

### Before you start
n this guide, a **privileged** user will be created and test databases with data will be set up.

It is recommended to follow this quick start guide in a non-production environment.

#### Prerequisites:
- A working PostgreSQL instance
- PostgreSQL client utilities installed 

### 1. Preparing pg_anon 
```bash
git clone https://github.com/TantorLabs/pg_anon.git pg_anon
cd pg_anon
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python3 -m pg_anon --version
```

### 2. Preparing DB environment
**Create a DB user for the quick start guide**
```bash
sudo su - postgres -c "psql -p 5432 -U postgres -c \"CREATE USER anon_test_user WITH PASSWORD 'mYy5RexGsZ' SUPERUSER;\""
```

**Prepare the SQL script to initialize the databases**
```bash
cat > /tmp/db_env.sql << 'EOL'
DROP DATABASE IF EXISTS pg_anon_quick_start_source_db;
CREATE DATABASE pg_anon_quick_start_source_db
WITH
OWNER = anon_test_user
ENCODING = 'UTF8'
LC_COLLATE = 'en_US.UTF-8'
LC_CTYPE = 'en_US.UTF-8'
template = template0;

DROP DATABASE IF EXISTS pg_anon_quick_start_target_db;
CREATE DATABASE pg_anon_quick_start_target_db
WITH
OWNER = anon_test_user
ENCODING = 'UTF8'
LC_COLLATE = 'en_US.UTF-8'
LC_CTYPE = 'en_US.UTF-8'
template = template0;
EOL
```

**Initialize source and target databases**
```bash
sudo chown postgres:postgres /tmp/db_env.sql
sudo su - postgres -c "psql -p 5432 -U postgres -f /tmp/db_env.sql"
```

**Load test environment into source DB**
```bash
cp $(pwd)/tests/init_env.sql /tmp/init_env.sql
sudo chown postgres:postgres /tmp/init_env.sql
sudo su - postgres -c "psql -p 5432 -d pg_anon_quick_start_source_db -U postgres -f /tmp/init_env.sql"
```

### 3. Initializing the service schema for pg_anon
```bash
python3 -m pg_anon --mode=init \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_source_db \
	--db-host=127.0.0.1 \
	--db-port=5432
```


### 4. Scan your source database
```bash
python3 -m pg_anon --mode=create-dict \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_source_db \
	--db-host=127.0.0.1 \
	--db-port=5432 \
	--meta-dict-file=tests/input_dict/test_meta_dict.py \
	--output-sens-dict-file=test_sens_dict_output.py \
	--output-no-sens-dict-file=test_no_sens_dict_output.py \
	--processes=2
```

### 5. Visualizing anonymization rules

Run pg_anon in `view-fields` mode to see which fields will be anonymized and which fields will be dumped as-is.

```bash
python3 -m pg_anon --mode=view-fields \
	--db-host=127.0.0.1 \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_source_db \
	--db-port=5432 \
	--prepared-sens-dict-file=test_sens_dict_output.py \
	--fields-count=20
```

Run pg_anon in `view-data` mode to preview anonymized data in a specific table.
```bash
python3 -m pg_anon --mode=view-data \
	--db-host=127.0.0.1 \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_source_db \
	--db-port=5432 \
	--prepared-sens-dict-file=test_sens_dict_output.py \
	--schema-name=public \
	--table-name=contracts \
	--limit=10 \
	--offset=0
```

### 6. Create an anonymized backup
```bash
python3 -m pg_anon --mode=dump \
	--db-host=127.0.0.1 \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_source_db \
	--db-port=5432 \
	--prepared-sens-dict-file=test_sens_dict_output.py \
	--output-dir=/tmp/quick_start_dump \
	--clear-output-dir
```

### 7. Restore the anonymized backup into the target DB
```bash
python3 -m pg_anon --mode=restore \
	--db-host=127.0.0.1 \
	--db-port=5432 \
	--db-user=anon_test_user \
	--db-user-password=mYy5RexGsZ \
	--db-name=pg_anon_quick_start_target_db \
	--input-dir=/tmp/quick_start_dump \
	--drop-custom-check-constr \
	--verbose=debug
```

---

## 📘 Documentation Index
| Section                                                                 | Description                                                       |
|-------------------------------------------------------------------------|-------------------------------------------------------------------|
| [💽 Installation & Configuration](docs/installation-and-configuring.md) | How to install and configure `pg_anon`                            |
| [⚙️ How It Works](docs/how-it-works.md)                                 | Describing anonymizations process into `pg_anon`                  |
| [🛠️ Debugging](docs/debugging.md)                                      | How to debug anonymizations process                               |
| [💬 FAQ](docs/faq.md)                                                   | Common questions and troubleshooting tips                         |
| [📚 SQL Functions Library](docs/sql-functions-library.md)               | Built-in SQL functions for anonymization                          |
| [🔌 API](docs/api.md)                                                   | Available endpoints, request/response formats, and usage examples |
| [💡 Contributing](docs/contributing.md)                                 | Info about contributing                                           |

### 📘 Operations
| Operation                                         | Description                                                                            |
|---------------------------------------------------|----------------------------------------------------------------------------------------|
| [🏗️ Init](docs/operations/init.md)               | Initialize schema `anon_funcs` with sql functions. It used for scan and dump processes |
| [🔍 Create-dict (Scan)](docs/operations/scan.md)  | Analyze your database and detect sensitive data                                        |
| [💾 Dump](docs/operations/dump.md)                | Export and anonymize data using prepared dictionaries                                  |
| [📂 Restore](docs/operations/restore.md)          | Load anonymized data into a target database                                            |
| [🔬 View Fields](docs/operations/view-fields.md) | Inspect anonymized fields or test the anonymization pipeline                           |
| [📊 View Data](docs/operations/view-data.md)     | Inspect anonymized data or test the anonymization pipeline                             |

### 📘 Dictionary Schemas
| Dictionary type                                                   | Description                                                        |
|-------------------------------------------------------------------|--------------------------------------------------------------------|
| [🗂️ Meta Dictionary](docs/dicts/meta-dict-schema.md)             | Structure of the meta-dictionary used for scanning                  |
| [🔐 Sensitive Dictionary](docs/dicts/sens-dict-schema.md)         | Structure of sensitive dictionaries                       |
| [📋 Non-sensitive Dictionary](docs/dicts/non-sens-dict-schema.md) | Structure of non-sensitive dictionaries                   |
| [📑 Tables dictionary](docs/dicts/tables-dictionary.md)           | Dictionary structure for partial dump/restore operations           |