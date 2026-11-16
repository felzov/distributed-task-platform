"""The docs stay true to the repository: the tests they name as `file::test` exist (a bare file
path is not checked), their relative links point at files that exist, and `.env.example` lists
every setting, each commented-out default the real one.

Each check is a function over text with a test of its own on a small bad input, and runs on the
real files. The docs' references and links must all be in the formats the checks read, so a
reference or link written another way fails instead of going unchecked.
"""

import ast
import re
from pathlib import Path

import pytest

from task_platform.config import BrokerSettings

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
DOCS = sorted([ROOT / "README.md", *(ROOT / "docs").rglob("*.md")])

# `integration/test_x.py::test_name` or `…::test_name[param-id]`, inside backticks, under tests/.
TEST_REFERENCE = re.compile(r"`([\w/]+\.py)::(test_\w+)(?:\[([^\]`]+)\])?`")
# Anything that looks like a reference to a test, in whatever format.
LOOSE_TEST_REFERENCE = re.compile(r"::\s*test_\w+")
# [text](target): inline links without a title; the docs use no reference-style links.
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
# Anything that looks like the middle of an inline link.
LOOSE_LINK = re.compile(r"\]\(")
# `NAME=value` or `# NAME=value`, at the start of a line.
ENV_LINE = re.compile(r"^(#\s*)?([A-Z][A-Z0-9_]*)=(\S*)", re.MULTILINE)
# Variables in .env.example that are not application settings: Compose's container passwords
# and the integration tests' URLs.
NOT_SETTINGS = {"POSTGRES_PASSWORD", "RABBITMQ_PASSWORD", "TEST_DATABASE_URL", "TEST_BROKER_URL"}


def defined_tests(path: Path) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    """The test functions pytest would collect by name: module level, or methods of a class."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [*tree.body]
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            nodes.extend(node.body)
    return {
        node.name: node
        for node in nodes
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.name.startswith("test_")
    }


def parameter_ids(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """The strings in the `ids=` of the function's decorators: its explicit parameter ids, not its
    parameter names or values."""
    return {
        child.value
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        for keyword in decorator.keywords
        if keyword.arg == "ids"
        for child in ast.walk(keyword.value)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }


def missing_test_references(text: str, tests_root: Path) -> list[str]:
    """Every `file::test[id]` reference in `text` whose file, function or parameter id does not
    exist under `tests_root`. A parameter id must be in that test's own `ids=`, so
    ids pytest generates from values are reported missing: the docs name explicit ids only."""
    missing = []
    for file, name, param in TEST_REFERENCE.findall(text):
        path = tests_root / file
        tests = defined_tests(path) if path.is_file() else None
        if tests is None:
            missing.append(f"{file} (no such file)")
        elif name not in tests:
            missing.append(f"{file}::{name}")
        elif param and param not in parameter_ids(tests[name]):
            missing.append(f"{file}::{name}[{param}]")
    return missing


def unread(text: str, loose: re.Pattern[str], strict: re.Pattern[str]) -> list[str]:
    """The lines holding a `loose` match that no `strict` match covers: a reference or link in a
    format the checks would skip."""
    covered = [match.span() for match in strict.finditer(text)]
    lines = []
    for match in loose.finditer(text):
        if not any(start <= match.start() < end for start, end in covered):
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_end = text.find("\n", match.start())
            lines.append(text[line_start : line_end if line_end != -1 else None].strip())
    return lines


def broken_links(text: str, base: Path) -> list[str]:
    """Every relative link in `text` that does not point at a file (a directory does not count),
    relative to `base`. External links (with a scheme) and in-page anchors are not checked; nor is
    a link's `#anchor` part."""
    broken = []
    for target in LINK.findall(text):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("#"):
            continue
        path = target.split("#", 1)[0]
        if not (base / path).is_file():
            broken.append(target)
    return broken


def env_example_problems(text: str) -> list[str]:
    """What `.env.example` gets wrong about the settings: a setting it does not list, a variable
    listed twice or not a setting, or a commented-out value that is not the setting's default
    (the file's commented-out overrides show the defaults). Uncommented values are examples."""
    entries = ENV_LINE.findall(text)
    listed = {name: (bool(comment), value) for comment, name, value in entries}
    fields = BrokerSettings.model_fields
    problems = [f"{name.upper()} not listed" for name in fields if name.upper() not in listed]
    names = [name for _, name, _ in entries]
    problems += [f"{name} listed twice" for name in dict.fromkeys(names) if names.count(name) > 1]
    for env_name, (commented, value) in listed.items():
        if env_name in NOT_SETTINGS:
            continue
        field = fields.get(env_name.lower())
        if field is None:
            problems.append(f"{env_name} is not a setting")
        elif commented and value != str(field.default):
            problems.append(f"{env_name} shows {value}, the default is {field.default}")
    return problems


def test_a_missing_test_reference_is_found(tmp_path: Path) -> None:
    (tmp_path / "integration").mkdir()
    (tmp_path / "integration" / "test_a.py").write_text(
        '@pytest.mark.parametrize("x", [1], ids=["one"])\n'
        "async def test_exists(x): ...\n"
        '@pytest.mark.parametrize("x", ["four"], ids=["three"])\n'
        "def test_other(x): ...\n"
        "class TestGroup:\n"
        "    def test_method(self): ...\n"
        "def helper():\n"
        "    def test_nested(): ...\n"
    )
    text = (
        "`integration/test_a.py::test_exists`, `integration/test_a.py::test_exists[one]`, "
        "`integration/test_a.py::test_method`, "
        "`integration/test_a.py::test_gone`, `integration/test_a.py::test_exists[two]`, "
        "`integration/test_a.py::test_exists[three]`, `integration/test_a.py::test_other[four]`, "
        "`integration/test_a.py::test_nested`, "
        "`integration/test_b.py::test_exists`"
    )
    assert missing_test_references(text, tmp_path) == [
        "integration/test_a.py::test_gone",
        "integration/test_a.py::test_exists[two]",
        # Another test's id does not count for this one.
        "integration/test_a.py::test_exists[three]",
        # A parameter value is not an id.
        "integration/test_a.py::test_other[four]",
        # Not collected by pytest: nested in a function.
        "integration/test_a.py::test_nested",
        "integration/test_b.py (no such file)",
    ]


def test_a_reference_in_another_format_is_found() -> None:
    text = "\n".join(
        [
            "Read: `integration/test_a.py::test_exists` and `integration/test_a.py::test_b[x]`.",
            "No backticks: integration/test_a.py::test_exists",
            "A prefix: `tests/integration/test-a.py::test_exists`",
            "A class: `integration/test_a.py::Group::test_exists`",
            "A space: `integration/test_a.py:: test_exists`",
            "Prose, not a reference: `file::test`",
        ]
    )
    assert unread(text, LOOSE_TEST_REFERENCE, TEST_REFERENCE) == [
        "No backticks: integration/test_a.py::test_exists",
        "A prefix: `tests/integration/test-a.py::test_exists`",
        "A class: `integration/test_a.py::Group::test_exists`",
        "A space: `integration/test_a.py:: test_exists`",
    ]


def test_a_broken_link_is_found(tmp_path: Path) -> None:
    (tmp_path / "adr").mkdir()
    (tmp_path / "adr" / "0001.md").write_text("")
    text = (
        "[ok](adr/0001.md) [ok with anchor](adr/0001.md#decision) [anchor](#top) "
        "[web](https://example.com/x) [gone](adr/0002.md) [directory](adr/)"
    )
    assert broken_links(text, tmp_path) == ["adr/0002.md", "adr/"]


def test_a_link_in_another_format_is_found() -> None:
    text = "\n".join(
        [
            "Read: [a](x.md) and [b](https://example.com).",
            'A title: [a](x.md "the title")',
            "Angle brackets: [a](<x y.md>)",
            "Brackets in the text: [`f[1]`](x.md)",
        ]
    )
    assert unread(text, LOOSE_LINK, LINK) == [
        'A title: [a](x.md "the title")',
        "Angle brackets: [a](<x y.md>)",
        "Brackets in the text: [`f[1]`](x.md)",
    ]


def test_env_example_problems_are_found() -> None:
    text = "\n".join(
        [
            *(
                f"{name.upper()}=x"
                for name in BrokerSettings.model_fields
                if name != "database_url"
            ),
            "# DB_POOL_SIZE=7",
            "# RETRY_BASE_DELAY_S=2.0",
            "# NO_SUCH_SETTING=1",
            "POSTGRES_PASSWORD=change-me",
        ]
    )
    assert env_example_problems(text) == [
        "DATABASE_URL not listed",
        "DB_POOL_SIZE listed twice",
        "RETRY_BASE_DELAY_S listed twice",
        "DB_POOL_SIZE shows 7, the default is 5",
        "NO_SUCH_SETTING is not a setting",
    ]


@pytest.mark.parametrize("doc", DOCS, ids=lambda path: str(path.relative_to(ROOT)))
def test_every_test_a_doc_names_exists(doc: Path) -> None:
    text = doc.read_text(encoding="utf-8")
    assert unread(text, LOOSE_TEST_REFERENCE, TEST_REFERENCE) == []
    assert missing_test_references(text, TESTS) == []


@pytest.mark.parametrize("doc", DOCS, ids=lambda path: str(path.relative_to(ROOT)))
def test_every_relative_link_in_a_doc_resolves(doc: Path) -> None:
    text = doc.read_text(encoding="utf-8")
    assert unread(text, LOOSE_LINK, LINK) == []
    assert broken_links(text, doc.parent) == []


def test_the_env_example_lists_every_setting_with_its_default() -> None:
    assert env_example_problems((ROOT / ".env.example").read_text(encoding="utf-8")) == []
