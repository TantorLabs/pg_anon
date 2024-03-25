from datetime import datetime
import os

from typing import Dict, Union, Any
from pydantic import BaseModel, PlainSerializer
from typing_extensions import Annotated


class SequenceMetaData(BaseModel):
    schema: str
    seq_name: str
    value: int


class FilesMetaData(BaseModel):
    schema: str
    table: str
    rows: str


class MetaData(BaseModel):
    db_size: int
    created: datetime
    seq_lastvals: Dict[str, SequenceMetaData]
    pg_version: str
    dic1: Dict[Any, str]
    pg_dump_version: str
    dictionary_content_hash: str
    dict_file: str
    files: Dict[str, FilesMetaData]
    total_tables_size: int
    total_rows: int
