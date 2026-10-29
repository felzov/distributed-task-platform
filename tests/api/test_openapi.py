"""The OpenAPI document builds and describes the hand-read `POST /jobs` body."""

import json

from task_platform.main import create_app
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_settings


async def test_submit_request_body_schema_is_inline_and_complete() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    request_body = spec["paths"]["/jobs"]["post"]["requestBody"]
    schema = request_body["content"]["application/json"]["schema"]
    assert request_body["required"] is True
    assert set(schema["required"]) == {"type", "payload_version", "payload"}
    assert schema["additionalProperties"] is False
    # The schema is inlined through openapi_extra, so a $ref inside it would resolve against the
    # document root, where its $defs do not exist.
    assert "$ref" not in json.dumps(schema)


async def test_error_responses_use_the_envelope() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    envelope = {"$ref": "#/components/schemas/ErrorResponse"}
    expected = {
        ("/jobs", "post"): {"413", "422", "500", "503"},
        ("/jobs/{job_id}", "get"): {"404", "422", "500", "503"},
    }
    for (path, method), codes in expected.items():
        responses = spec["paths"][path][method]["responses"]
        for code in codes:
            assert responses[code]["content"]["application/json"]["schema"] == envelope
    # FastAPI documents its own 422 body unless a route lists 422; ours replaces it everywhere.
    assert "HTTPValidationError" not in json.dumps(spec)


async def test_ready_documents_its_503_envelope() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    response = spec["paths"]["/ready"]["get"]["responses"]["503"]
    assert response["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ErrorResponse"
    }


async def test_probes_document_their_exact_status() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    for path, value in (("/health", "ok"), ("/ready", "ready")):
        ref = spec["paths"][path]["get"]["responses"]["200"]["content"]["application/json"]
        name = ref["schema"]["$ref"].rsplit("/", 1)[-1]
        assert spec["components"]["schemas"][name]["properties"]["status"]["const"] == value
