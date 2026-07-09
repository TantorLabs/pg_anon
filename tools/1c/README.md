# 1C:Enterprise helper tools

## DatabaseStructure.epf

An external data processor for 1C:Enterprise 8.3 that
exports the database storage structure to a JSON file compatible with the
`--orm-dict-file` option of the [view-fields mode](../../docs/operations/view-fields.md).

The exported file maps physical SQL names (tables and columns) to their 1C
metadata names, for example:

```json
{
    "_Reference77815X1": {
        "TableName": "Справочник.НастройкаОбмена",
        "TablePurpose": "Основная",
        "Fields": {
            "_IDRRef": "Ссылка",
            "_Fld77818": "COMИмяБазы"
        }
    }
}
```

### Usage

1. Open the data processor in the 1C:Enterprise mode of the target infobase:
   *File → Open → DatabaseStructure.epf*.
2. Run the export and save the resulting JSON file.
3. Pass the file to pg_anon:

```commandline
pg_anon view-fields \
    --db-host=127.0.0.1 \
    --db-user=postgres \
    --db-user-password=postgres \
    --db-name=source_db \
    --prepared-sens-dict-file=sens_dict.py \
    --orm-dict-file=DatabaseStructure.json
```

A typical 1C database contains about 15,000 tables; the export covers all of
them, so the resulting file can be reused for any pg_anon run against that
infobase (until the configuration changes).
