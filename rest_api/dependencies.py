from datetime import date
from pathlib import Path
from typing import Annotated

from fastapi import HTTPException, Query, status

from pg_anon.common.constants import RUNS_BASE_DIR


def date_range_filter(
    date_before: Annotated[date | None, Query(None, description="Filter: operations before this date")],
    date_after: Annotated[date | None, Query(None, description="Filter: operations after this date")],
):
    if date_before and date_after and date_after > date_before:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="`date_after` must be less than or equal to `date_before`",
        )
    return {"date_before": date_before, "date_after": date_after}


def get_operation_run_dir(internal_operation_id: str) -> Path:
    for run_dir in RUNS_BASE_DIR.glob(f"*/*/*/{internal_operation_id}"):
        return run_dir

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Operation run directory not found",
    )
