import sys

from uvicorn.main import main as uvicorn_main


def main():
    sys.argv = ["uvicorn", "rest_api.api:app", *sys.argv[1:]]
    uvicorn_main()


if __name__ == "__main__":
    main()
