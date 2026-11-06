# http-api Specification

## Purpose
The HTTP API: v2 scan and classify routes, the pre-1.0 scan_data route, limits and readiness.
## Requirements
### Requirement: HTTP API v2

The `server` extra SHALL provide an HTTP API with these routes:
- `POST /v2/classify` (column samples);
- `POST /v2/scan` (records);
- `GET /health`;
- `GET /ready`.

The API SHALL publish an OpenAPI description at `/openapi.json`. Both POST routes SHALL
accept the same options as the SDK and SHALL return report v2.

#### Scenario: Same result as the SDK

- **WHEN** the same records and options are sent to `POST /v2/scan` and passed to the SDK
- **THEN** both return equal report v2 objects, apart from timestamps

#### Scenario: Readiness

- **WHEN** rules and registry data have finished loading
- **THEN** `GET /ready` returns 200
- **AND** before that it returns 503

### Requirement: HTTP Request Limits

The HTTP API SHALL reject request bodies above the configured size with status 413. It
SHALL reject malformed payloads or record counts above the limit with status 400 and a
message that names the violated limit.

#### Scenario: Wrong payload shape

- **WHEN** a client posts the JSON scalar `3` to `POST /v2/scan`
- **THEN** the API returns 400 with a validation message

### Requirement: Legacy v1 Endpoint Compatibility

The HTTP API SHALL keep `POST /api/v1/scan_data` with its pre-1.0 request parameters and
response structure for at least two minor releases after 1.0.0.

#### Scenario: Existing client

- **WHEN** an existing client posts records to `/api/v1/scan_data?format=short`
- **THEN** the response has the pre-1.0 structure

