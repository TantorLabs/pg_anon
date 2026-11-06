from __future__ import annotations

import pytest

from pg_anon.common.dto import FieldInfo
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.modes.create_dict import CreateDictMode

FUNC_F = "func_f"
FUNC_G = "func_g"

FIELD_INFO = FieldInfo("public", "t", "c", "text", 1, 1, "obj", "tbl")


def _check_regex(mode, dictionary, data):
    return mode._check_data_by_regexp("t", dictionary, {}, FIELD_INFO, data)  # noqa: SLF001


def _check_constants(mode, dictionary, data):
    return mode._check_data_by_constants("t", dictionary, FIELD_INFO, data)  # noqa: SLF001


def _check_partial_constants(mode, dictionary, data):
    return mode._check_data_by_partial_constants("t", dictionary, FIELD_INFO, data)  # noqa: SLF001


def _rules(*items: tuple[str, str]) -> list[dict]:
    return [{"rule": rule, "anon_func": func} for rule, func in items]


# Rules r0(F), r1(G), r2(F): value 1 matches only r2, value 2 matches r0 and r1
CHECK_CASES = {
    "data_regex": (
        {"data_regex": {"rules": _rules(("a", FUNC_F), ("b", FUNC_G), ("c", FUNC_F))}},
        _check_regex,
        ["c", "ab"],
    ),
    "constants": (
        {"data_const": {"constants": _rules(("alpha", FUNC_F), ("beta", FUNC_G), ("gamma", FUNC_F))}},
        _check_constants,
        ["gamma", "alpha beta"],
    ),
    "phrases": (
        {"data_const": {"constants": _rules(("red apple", FUNC_F), ("big pear", FUNC_G), ("sour plum", FUNC_F))}},
        _check_constants,
        ["a sour plum", "a red apple and a big pear"],
    ),
    "partial_constants": (
        {"data_const": {"partial_constants": _rules(("aa", FUNC_F), ("bb", FUNC_G), ("cc", FUNC_F))}},
        _check_partial_constants,
        ["xccx", "xaabbx"],
    ),
}


@pytest.mark.parametrize("reverse_values", [False, True], ids=["forward", "reversed"])
@pytest.mark.parametrize("section", CHECK_CASES)
def test_first_rule_of_the_list_wins_for_any_order_of_values(section, reverse_values, write_dict, make_context):
    meta_dict, check, values = CHECK_CASES[section]
    ctx = make_context(meta_dict_files=[write_dict(meta_dict)])
    ctx.read_meta_dict()

    rule = check(CreateDictMode(ctx), ctx.meta_dictionary_obj, values[::-1] if reverse_values else values)

    assert rule is not None
    assert rule.anon_func == FUNC_F
    assert rule.position == 0


@pytest.mark.parametrize("reverse_values", [False, True], ids=["forward", "reversed"])
def test_string_rule_above_object_rule_gives_no_function_for_any_order_of_values(
    reverse_values, write_dict, make_context
):
    meta_dict = {"data_const": {"constants": ["Ромашка", {"rule": "Лютик", "anon_func": FUNC_F}]}}
    ctx = make_context(meta_dict_files=[write_dict(meta_dict)])
    ctx.read_meta_dict()
    values = ["Ромашка", "Лютик"]

    rule = _check_constants(CreateDictMode(ctx), ctx.meta_dictionary_obj, values[::-1] if reverse_values else values)

    assert rule is not None
    assert rule.anon_func is None


@pytest.mark.parametrize(
    ("section", "meta_dict"),
    [
        ("field.rules", {"field": {"rules": [{"rule": "^a$", "func": FUNC_F}]}}),
        ("field.constants", {"field": {"constants": [{"rule": "a"}]}}),
        ("data_regex.rules", {"data_regex": {"rules": [{"rule": "a", "anon_func": FUNC_F, "extra": "x"}]}}),
        ("data_const.constants", {"data_const": {"constants": [{"anon_func": FUNC_F}]}}),
        ("data_const.partial_constants", {"data_const": {"partial_constants": [{"rule": "a", "anon_func": 1}]}}),
    ],
)
def test_rule_object_with_wrong_keys_or_values_is_invalid(section, meta_dict, write_dict, make_context):
    ctx = make_context(meta_dict_files=[write_dict(meta_dict)])

    with pytest.raises(PgAnonError) as error:
        ctx.read_meta_dict()

    assert error.value.code == ErrorCode.INVALID_META_DICT
    assert section in error.value.message
