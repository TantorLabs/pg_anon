import sys


def main() -> None:
    """Launch the REST API server via uvicorn."""
    try:
        from uvicorn.main import main as uvicorn_main
    except ImportError:
        sys.exit("REST API dependencies not installed. Run: pip install pg_anon[api]")
    sys.argv = ["uvicorn", "pg_anon.rest_api.api:app", *sys.argv[1:]]
    uvicorn_main()


if __name__ == "__main__":
    main()
