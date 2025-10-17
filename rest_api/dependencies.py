from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import HTTPException, Query, status

from pg_anon.common.constants import RUNS_BASE_DIR, SAVED_DICTS_INFO_FILE_NAME


def date_range_filter(
    date_before: Optional[date] = Query(None, description="Filter: operations before this date"),
    date_after: Optional[date] = Query(None, description="Filter: operations after this date"),
):
    if date_before and date_after and date_after > date_before:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="`date_after` must be less than or equal to `date_before`",
        )
    return {"date_before": date_before, "date_after": date_after}


def get_operation_run_dir(internal_operation_id: str) -> Path:
    run_dir = None
    for path in RUNS_BASE_DIR.glob(f'*/*/*/{internal_operation_id}'):
        run_dir = path

    if not run_dir:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation run directory not found",
        )

    return run_dir
