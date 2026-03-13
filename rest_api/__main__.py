import sys

from uvicorn.main import main as uvicorn_main


def main() -> None:
    """Launch the REST API server via uvicorn."""
    sys.argv = ["uvicorn", "rest_api.api:app", *sys.argv[1:]]
    uvicorn_main()


if __name__ == "__main__":
    main()
