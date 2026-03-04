# Admin Connectors

Scanner connectors define ingestion adapters, not a source whitelist.

## Status values
- `live`: connector is implemented and can ingest now.
- `pending`: connector requires a key and implementation pass.
- `stub`: placeholder visible in UI for planning/coverage.
- `disabled`: connector intentionally off.

## Notes
- Discovery is automatic.
- Enabled state is persisted in `scanner_connectors`.
- Sources modal always lists known connectors for the selected group, including zero-count connectors.
