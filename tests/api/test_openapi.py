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
        ("/jobs", "get"): {"422", "500", "503"},
        ("/jobs/{job_id}", "get"): {"404", "422", "500", "503"},
        ("/jobs/{job_id}/attempts", "get"): {"404", "422", "500", "503"},
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


async def test_job_response_documents_result_and_failure() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    schemas = spec["components"]["schemas"]
    ok = spec["paths"]["/jobs/{job_id}"]["get"]["responses"]["200"]["content"]["application/json"]
    assert ok["schema"] == {"$ref": "#/components/schemas/JobResponse"}
    job = schemas["JobResponse"]
    assert {"result", "failure"} <= set(job["required"])
    assert job["properties"]["result"]["anyOf"] == [
        {"type": "object", "additionalProperties": True},
        {"type": "null"},
    ]
    assert job["properties"]["failure"]["anyOf"] == [
        {"$ref": "#/components/schemas/FailureBody"},
        {"type": "null"},
    ]
    assert set(schemas["FailureBody"]["required"]) == {"code", "message"}


async def test_attempts_response_documents_each_attempt() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    schemas = spec["components"]["schemas"]
    path = "/jobs/{job_id}/attempts"
    ok = spec["paths"][path]["get"]["responses"]["200"]["content"]["application/json"]
    assert ok["schema"] == {"$ref": "#/components/schemas/AttemptsResponse"}
    assert schemas["AttemptsResponse"]["properties"]["attempts"]["items"] == {
        "$ref": "#/components/schemas/AttemptBody"
    }
    attempt = schemas["AttemptBody"]
    assert set(attempt["required"]) == {
        "attempt",
        "started_at",
        "finished_at",
        "outcome",
        "failure",
    }
    # Null while the attempt runs; otherwise one of the stored outcomes.
    assert attempt["properties"]["outcome"]["anyOf"] == [
        {"$ref": "#/components/schemas/AttemptOutcome"},
        {"type": "null"},
    ]
    assert set(schemas["AttemptOutcome"]["enum"]) == {"succeeded", "failed"}
    assert attempt["properties"]["failure"]["anyOf"] == [
        {"$ref": "#/components/schemas/FailureBody"},
        {"type": "null"},
    ]


async def test_job_list_documents_its_query_and_page() -> None:
    app = create_app(make_test_settings(UNREACHABLE_DATABASE_URL))
    try:
        spec = app.openapi()
    finally:
        await app.state.engine.dispose()

    operation = spec["paths"]["/jobs"]["get"]
    parameters = {p["name"]: p for p in operation["parameters"]}
    assert set(parameters) == {"status", "limit", "cursor"}
    assert parameters["status"]["required"] is True
    # Only failed jobs can be listed so far.
    assert parameters["status"]["schema"]["const"] == "failed"
    limit = parameters["limit"]["schema"]
    assert (limit["minimum"], limit["maximum"], limit["default"]) == (1, 100, 50)
    ok = operation["responses"]["200"]["content"]["application/json"]
    assert ok["schema"] == {"$ref": "#/components/schemas/JobListResponse"}
    page = spec["components"]["schemas"]["JobListResponse"]
    assert set(page["required"]) == {"jobs", "next_cursor"}
    # Each job has the same shape as GET /jobs/{id}, so never a payload.
    assert page["properties"]["jobs"]["items"] == {"$ref": "#/components/schemas/JobResponse"}
