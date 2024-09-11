import re


def simple_slugify(value: str):
    return re.sub(r'\W+', '-', value).strip('-').lower()
