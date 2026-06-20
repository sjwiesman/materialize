---
source: src/mz-deploy/src/project/ir/infrastructure.rs
revision: a647094cc4
---

# mz_deploy::project::ir::infrastructure

Extracts structured infrastructure metadata from connection, source, and
table-from-source statements. Consumed by the compiler (to persist to SQLite) and
by the LSP catalog (to build the explore page).

`Infrastructure` is the output enum with three variants: `Connection`
(`connector_type` plus a list of `Property` values), `Source` (`connector_type`,
an optional `connection_ref` for linking, and properties), and `TableFromSource`
(the `source_ref` and an optional `external_reference`). `Property` is a key-value
pair carrying a display `value` and optional `secret_ref` / `object_ref` fields
that hold the referenced name for downstream linking when a value points at a
secret or another object.

`extract` is the entry point: it returns `Some` for `CreateConnection`,
`CreateSource`, and `CreateTableFromSource` statements and `None` for everything
else. `extract_connection` maps the connection type to a human-readable name via
`connection_type_name` and converts its options to properties, filtering out the
auto-generated `PublicKey1` / `PublicKey2` SSH key material. `extract_source`
matches on the `CreateSourceConnection` to determine the connector type and
connection reference (Postgres, Kafka, MySQL, SQL Server, or a Load Generator with
its generator name) and converts its options. `extract_table_from_source` pulls
the source reference and optional external reference. `format_option_value`
renders a `WithOptionValue` to a display string and surfaces secret/object refs,
`raw_item_name_to_string` extracts the unqualified name from a `RawItemName`, and
the `options_to_properties!` macro maps a slice of options into `Property` entries,
skipping options with no value.
