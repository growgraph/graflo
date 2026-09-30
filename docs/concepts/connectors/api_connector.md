# API connector

An API [connector](../glossary.md#connector) reads JSON records from a REST endpoint, for example the work orders that a plant's maintenance system serves over HTTP. Use it when the records you want in the graph sit behind an API rather than in files or a database. This page shows how to declare an endpoint, where its address and credentials go, and how to read a result that the API returns in pages.

## A first example

Suppose the maintenance system answers `GET /api/work-orders?offset=0&limit=100` with:

```json
{
  "data": [
    {"id": "WO-1001", "machine_serial": "SN-4471", "status": "open"},
    {"id": "WO-1002", "machine_serial": "SN-2210", "status": "closed"}
  ],
  "has_more": true
}
```

Add this `bindings` block to a [manifest](../glossary.md#manifest) that has a `work_orders` [resource](../glossary.md#resource):

```yaml
bindings:
  connectors:
    - name: work_orders_api
      path: /api/work-orders
      resource_name: work_orders
      pagination:
        request:
          strategy: offset
          page_size: 100
        response:
          records_path: data
          has_more_path: has_more
  connector_connection:
    - connector: work_orders_api
      conn_proxy: maintenance_api
```

- `path` is appended to the base URL of the API.
- `resource_name` sends every record to the `work_orders` resource.
- `pagination` asks for 100 records per request (`offset=0&limit=100`, then `offset=100&limit=100`, and so on), takes the records from `data`, and stops when `has_more` is false.
- `conn_proxy: maintenance_api` is a [connection proxy](../glossary.md#connection-proxy): a label that stands for the base URL and the credentials, so the manifest holds neither.

Set the base URL and the token for that label:

```bash
export MAINTENANCE_API_BASE_URL=https://maintenance.example.com
export MAINTENANCE_API_TOKEN=your-token
```

Then load the manifest, register the connection from the environment, and run the ingestion:

```python
from pathlib import Path

from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.connections import GraFloBackendConfig, InMemoryConnectionProvider

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
bindings = manifest.require_bindings()

# Reads MAINTENANCE_API_BASE_URL and MAINTENANCE_API_TOKEN
provider = InMemoryConnectionProvider()
provider.register_all_api_configs_from_env(bindings=bindings)

target = GraFloBackendConfig(output_dir=Path("artifacts/work-orders"))
engine = GraphEngine(target_db_flavor=target.connection_type)
engine.define_and_ingest(
    manifest=manifest,
    target_db_config=target,
    connection_provider=provider,
    recreate_schema=True,
)
```

Each request goes to `https://maintenance.example.com/api/work-orders` with the header `Authorization: Bearer your-token`. This run writes the graph to the [file backend](../glossary.md#file-backend), a directory on disk, so you can try it without a database. To write to a database, pass its config, such as a `Neo4jConfig`, as `target_db_config`.

## Base URL and credentials

At run time a [connection provider](../glossary.md#connection-provider) maps each `conn_proxy` label to a `RestApiConnConfig`: a `base_url`, an optional `auth` (an `ApiAuth`), and optional `default_headers`. You fill the provider from environment variables or in Python.

### From environment variables

`register_all_api_configs_from_env` finds every label that an API connector uses, reads the variables for each label, and binds each connector to its label. The variable prefix is the label in upper case, with `-` replaced by `_`, followed by `_`: the label `maintenance_api` reads `MAINTENANCE_API_*`, and `sensor-feed` reads `SENSOR_FEED_*`.

| Variable | Required | Meaning |
|---|---|---|
| `{PREFIX}BASE_URL` | yes | Base URL. The connector's `path` is appended to it, so the base URL may carry a path of its own, such as `https://maintenance.example.com/v2`. |
| `{PREFIX}AUTH_TYPE` | no, default `bearer` | `bearer`, `api_key`, `basic` or `digest` |
| `{PREFIX}TOKEN` | for `bearer` and `api_key` | The token or key |
| `{PREFIX}USERNAME`, `{PREFIX}PASSWORD` | for `basic` and `digest` | User name and password |
| `{PREFIX}HEADER_NAME` | no, default `Authorization` | Header that carries the token or key |
| `{PREFIX}PREFIX` | no, default `Bearer` | Word written before the token (`bearer` only) |

A missing `{PREFIX}BASE_URL`, or an `AUTH_TYPE` other than these four, raises a `ValueError` that names the variable.

When your variables do not follow the label, map the label to the prefix you use:

```python
provider.register_all_api_configs_from_env(
    bindings=bindings,
    env_prefix_map={"maintenance_api": "PLANT_MAINTENANCE_"},
)
```

To register a single label, call `register_api_config_from_env`. It does not bind the connectors, so call `bind_from_bindings` after it:

```python
provider.register_api_config_from_env(conn_proxy="maintenance_api")
provider.bind_from_bindings(bindings=bindings)
```

### In Python

Register a `RestApiConnConfig` yourself when the values come from somewhere else, such as a secret store, or when the API needs no authentication:

```python
from graflo.connections import (
    ApiAuth,
    ApiGeneralizedConnConfig,
    InMemoryConnectionProvider,
    RestApiConnConfig,
)

provider = InMemoryConnectionProvider()
provider.register_generalized_config(
    conn_proxy="maintenance_api",
    config=ApiGeneralizedConnConfig(
        config=RestApiConnConfig(
            base_url="https://maintenance.example.com",
            auth=ApiAuth(
                auth_type="api_key", token="your-key", header_name="X-Api-Key"
            ),
            default_headers={"Accept": "application/json"},
        )
    ),
)
provider.bind_from_bindings(bindings=bindings)
```

`bind_from_bindings` attaches each connector to the label that its `connector_connection` row names. Leave out `auth` for an API without authentication. The environment route always builds an `ApiAuth`: with the default `bearer` type and no token, every request carries the header `Authorization: Bearer` with nothing after it.

### Authentication types

| `auth_type` | Fields | What each request carries |
|---|---|---|
| `bearer` (default) | `token`, `header_name`, `prefix` | The header `{header_name}: {prefix} {token}`, by default `Authorization: Bearer <token>` |
| `api_key` | `token`, `header_name` | The header `{header_name}: {token}`. Set `header_name`, for example to `X-Api-Key`; the default is `Authorization`. |
| `basic` | `username`, `password` | HTTP Basic authentication |
| `digest` | `username`, `password` | HTTP Digest authentication |

## Pagination

Most APIs return a large result in pages. `pagination` has two parts: `request` says how to ask for a page, and `response` says where the records and the paging signals sit in the JSON body. For each page, GraFlo:

1. Sends a request to the base URL plus `path`, with the connector's `params`, any [session tokens](#session-tokens-carry_params), and the paging parameters of the strategy.
2. Reads the records at `response.records_path`.
3. Decides from the response whether another page exists; see [When paging stops](#when-paging-stops).
4. Works out the next offset, page number or cursor.

### Strategies

| `strategy` | Query parameters sent | How the next page is found |
|---|---|---|
| `offset` (default) | `offset_param` = current offset, `limit_param` = `page_size` | The value at `next_offset_path` when the response has one; otherwise the offset grows by `page_size` |
| `page` | `page_param` = page number, `per_page_param` = `page_size` | The page number grows by one |
| `cursor` | `cursor_param` = the token, left out of the first request unless `initial_cursor` is set | The token at `cursor_path` |

An API that names its offset parameters `skip` and `take`:

```yaml
pagination:
  request:
    strategy: offset
    offset_param: skip
    limit_param: take
    page_size: 50
  response:
    records_path: data
    has_more_path: has_more
```

The requests carry `skip=0&take=50`, then `skip=50&take=50`, and so on. Set `limit_param: null` for an API that rejects a page-size parameter.

An API with numbered pages that answers `{"results": {"items": [...], "has_next_page": true}}`:

```yaml
pagination:
  request:
    strategy: page
    per_page_param: page_size
    page_size: 25
  response:
    records_path: results.items
    has_more_path: results.has_next_page
```

An API that returns a token for the next page, as in `{"items": [...], "pagination": {"next_cursor": "eyJpZCI6MTF9"}}`:

```yaml
pagination:
  request:
    strategy: cursor
    cursor_param: cursor
  response:
    records_path: items
    cursor_path: pagination.next_cursor
```

The cursor strategy sends no page-size parameter. If the API expects one, put it in the connector's `params`.

### Request fields

| Field | Default | Strategy | Meaning |
|---|---|---|---|
| `strategy` | `offset` | all | `offset`, `page` or `cursor` |
| `page_size` | `100` | offset, page | Records asked for per request |
| `offset_param` | `offset` | offset | Name of the offset parameter |
| `limit_param` | `limit` | offset | Name of the page-size parameter; `null` sends none |
| `initial_offset` | `0` | offset | Offset of the first request |
| `page_param` | `page` | page | Name of the page-number parameter |
| `per_page_param` | `per_page` | page | Name of the page-size parameter |
| `initial_page` | `1` | page | Number of the first page |
| `cursor_param` | `cursor` | cursor | Name of the cursor parameter |
| `initial_cursor` | `null` | cursor | Cursor sent with the first request |
| `carry_params` | `{}` | all | Session tokens to send back; see [Session tokens](#session-tokens-carry_params) |

### Response fields

All paths are [dot paths](#dot-paths-and-response-shapes) into the JSON body.

| Field | Default | Meaning |
|---|---|---|
| `records_path` | `null` | Where the list of records is, such as `data` or `results.items` |
| `has_more_path` | `null` | A true or false flag that says whether another page exists |
| `next_offset_path` | `null` | The offset to request next |
| `total_count_path` | `null` | Total number of records across all pages; used together with `offset_path` |
| `offset_path` | `null` | The offset of this page, as the API echoes it |
| `cursor_path` | `null` | The token for the next page |
| `batch_metadata_paths` | `{}` | Values of the response to copy into every record of the page, as `{record field: path}`, for example `{_batch_id: result_id}` |
| `auto_detect` | `false` | Fill the unset paths from the first response; see [Detecting paths](#detecting-paths-from-the-first-response) |

### When paging stops

After each page, GraFlo looks for the first configured signal in this order and decides by that signal alone:

| Order | Configured | Paging stops when |
|---|---|---|
| 1 | `has_more_path` | The value is false or missing |
| 2 | `next_offset_path` | The value is missing or `null` |
| 3 | `total_count_path` and `offset_path`, both present in the response | offset + records on this page reaches the total |
| 4 | `cursor_path`, with the cursor strategy | The value is missing or empty |
| 5 | none of the above | A page has no records |

Reading also stops when `IngestionParams.max_items` records have been read, and, with the cursor strategy, when a response holds no next cursor. An endpoint that ignores the paging parameters returns the same records on every page, so rule 5 never stops it: give such an endpoint one of the first four signals, or read it in one request as described next.

### One request

Without `pagination`, GraFlo sends a single request and expects the body to be a JSON array; each element becomes a record. An object body raises a `ValueError`, because only `pagination.response` can say where the records are. For an endpoint that returns everything in one object, such as `{"data": [...]}`, use the cursor strategy without a `cursor_path`: GraFlo reads the first page, finds no next cursor, and stops.

```yaml
pagination:
  request:
    strategy: cursor
  response:
    records_path: data
```

### Page size and batch size

`page_size` is what each request asks the API for. It comes from the connector because it is the value the endpoint is known to accept: many APIs cap their page size and reject a larger one. `IngestionParams.batch_size` replaces `page_size` only when you set `batch_size` yourself; the default batch size never does. Separately, a page with more records than `batch_size` is split into several batches for [casting](../glossary.md#casting).

`IngestionParams.max_items` caps the number of records read from the connector, across all pages. The last request asks only for the remaining records. Both `batch_size` and `max_items` are fields of `IngestionParams`, which you pass to `define_and_ingest` as `ingestion_params`.

## Dot paths and response shapes

A dot path is a list of segments joined by `.`. On an object, a segment is a key; on a list, it is a number that picks an element. `meta.next_cursor` reads `body["meta"]["next_cursor"]`, and `0.results` reads `body[0]["results"]`. A path that does not resolve yields no value.

| Shape of the body | Example | Configuration |
|---|---|---|
| An object that wraps the records | `{"results": [...], "count": 120}` | `records_path: results` |
| An array of records | `[{"id": "WO-1001"}, {"id": "WO-1002"}]` | No `pagination`, or `pagination` without `records_path` |
| An array that wraps one object | `[{"results": [...], "count": 120}]` | Start every path with `0.`, such as `records_path: 0.results` |

For the array that wraps one object:

```json
[
  {
    "results": [{"id": "WO-1001"}, {"id": "WO-1002"}],
    "count": 120,
    "offset": 0,
    "next_offset": 100
  }
]
```

```yaml
pagination:
  request:
    strategy: offset
    page_size: 100
  response:
    records_path: 0.results
    total_count_path: 0.count
    offset_path: 0.offset
    next_offset_path: 0.next_offset
```

The paging signals read these paths as they would on an object body.

### Detecting paths from the first response

With `auto_detect: true`, GraFlo reads the first response and fills each response path you left unset with the first of these top-level keys that the body has. It logs the paths it found at INFO level. Paths you set are kept.

| Path | Keys tried, in order |
|---|---|
| `records_path` | `results`, `data`, `items`, `records`, `entries`, `rows`; otherwise the only key that holds a list of objects |
| `has_more_path` | `has_more`, `hasMore` |
| `next_offset_path` | `next_offset`, `nextOffset` |
| `total_count_path` | `count`, `total`, `total_count` |
| `offset_path` | `offset`, `skip` |
| `cursor_path` | `next_cursor`, `cursor`, `next_page_token` |

Detection looks only at the top level of an object body, so it misses nested keys such as `pagination.next_cursor`. On an array that wraps one object it finds nothing, and each element of the array becomes one record: set the `0.` paths yourself for that shape. Links in the response, such as a `next` URL, are never followed; every request is built from the base URL and `path`.

## Query parameters and headers

`params` adds fixed query parameters to every request, for filters that do not change from page to page. The paging parameters are added next to them.

`headers` sets headers without secrets for every request. They are merged over the `default_headers` of the `RestApiConnConfig`; for a header set in both places, the connector's value wins.

```yaml
connectors:
  - name: open_work_orders
    path: /api/work-orders
    params:
      status: open
      plant: north
    headers:
      Accept: application/json
```

Every request of this connector carries `status=open&plant=north`. Requests have no body: `params` go in the query string whatever the `method`.

## Timeouts, retries and TLS

| Field | Default | Meaning |
|---|---|---|
| `method` | `GET` | HTTP method |
| `timeout` | `null` | Seconds to wait for the API; `null` waits without limit |
| `retries` | `0` | How many times a request is retried after a connection error or a status in `retry_status_forcelist` |
| `retry_backoff_factor` | `0.1` | Scales the pause between retries |
| `retry_status_forcelist` | `[500, 502, 503, 504]` | Status codes that trigger a retry |
| `verify` | `true` | Check the TLS certificate of the API |

Set a `timeout`: without one, a request to an API that stops answering waits forever. Retries follow the rules of urllib3, which retries on a status code only for idempotent methods such as `GET`, not for `POST`.

## Session tokens (`carry_params`)

Some search APIs return a session token with the first page and expect it back as a query parameter on the pages that follow. `carry_params` maps the name of that query parameter to the path of the token in the response:

```yaml
pagination:
  request:
    strategy: offset
    limit_param: null
    carry_params:
      results_id: 0.results_id
  response:
    records_path: 0.results
    next_offset_path: 0.next_offset
```

If the first response is `[{"results_id": "s-81", "results": [...], "next_offset": 100}]`, the second request carries `offset=100&results_id=s-81`. The value is read again from every page, so a token that changes stays current.

While `carry_params` is empty, GraFlo looks in each response for the fields `results_id`, `scroll_id`, `pit_id` and `search_id`, at the top level of an object body or under `0.` of an array body. Each one it finds is sent back under its own name on every later request. A field named `result_id` is not treated as a token. Set `carry_params` when the parameter name differs from the field name, or when the token sits elsewhere in the response.

## Row annotations

`row_annotations` adds constant fields to every record that the connector reads. A field that the record already has keeps its own value. Start the names with `_` so that they do not collide with the fields of the API. Fields copied by `batch_metadata_paths` also yield to the record's own fields, and take precedence over `row_annotations`.

A typical use: an endpoint returns links between several kinds of things as uniform `source` and `target` pairs, with nothing that says what kind each end is. Give each kind of link its own connector, stamp the types on its records, and let a [vertex router](../glossary.md#vertex-router) step read the type from the stamped field:

```yaml
bindings:
  connector_templates:
    - name: relations_base
      path: /api/relations
      resource_name: relations
      conn_proxy: maintenance_api
      pagination:
        request:
          page_size: 200
        response:
          records_path: data
          has_more_path: has_more
  connectors:
    - name: sensors_on_machines
      base: relations_base
      params: {kind: installed_on}
      row_annotations: {_src_type: sensor, _tgt_type: machine, _rel: installed_on}
    - name: machines_on_lines
      base: relations_base
      params: {kind: part_of}
      row_annotations: {_src_type: machine, _tgt_type: production_line, _rel: part_of}
```

```yaml
ingestion_model:
  resources:
    - name: relations
      pipeline:
        - vertex_router: {type_field: _src_type, role: src, from: {id: source}}
        - vertex_router: {type_field: _tgt_type, role: tgt, from: {id: target}}
        - edge: {source_role: src, target_role: tgt, relation_field: _rel}
```

A record `{"source": "TMP-12", "target": "SN-4471"}` from the first connector becomes a `sensor`, a `machine` and an `installed_on` edge between them. The two connectors share their path, resource, label and pagination through a template, described next.

`row_annotations` works on API and Kafka connectors. File, table and SPARQL connectors reject a non-empty value.

## Connector templates

The example above declares its shared settings once, under `connector_templates`, and each connector refers to them with `base:`. A connector made from a template gets every field of the template, with these rules:

- `params`, `row_annotations` and `headers` are merged key by key; the connector's keys win.
- Every other field, such as `pagination`, is replaced as a whole when the connector sets it.
- A template's `conn_proxy` adds a `connector_connection` row for each connector made from it, so such a connector needs a `name`. A row you write yourself for that connector wins, and the connector may also set its own `conn_proxy`.

Templates are expanded when the bindings are loaded. A `conn_proxy` at the top level of `bindings` is the label for every connector that has no `connector_connection` row.

## Errors

- An HTTP error, a connection failure that outlasts the retries, or a body that is not JSON ends the reading of that connector. GraFlo logs the error at ERROR level and keeps the records it has already read; the run raises no exception. Check the log after a run against an unreliable API.
- An object body with no `records_path`, set or detected, raises a `ValueError`.
- A connector whose label has no registered configuration is skipped with a warning in the log; the other connectors run as usual.

## What to read next

- [API sources from environment variables (example 12)](../../examples/api-env-config/index.md): several APIs, each configured from its own variables.
- [Runtime connector updates](runtime_updates.md): change a connector's fields after the manifest is loaded.
- [Kafka connector](kafka_connector.md): read JSON messages from Kafka topics, with the same labels and providers.
