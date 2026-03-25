# B Graph Build Checklist

## Phase 0 — Repo setup
- [ ] Confirm folder structure
- [ ] Add `.env.example`
- [ ] Add README with local run steps
- [ ] Confirm dataset folders/files in `data/raw/`

## Phase 1 — Data layer
- [x] Inspect all JSON files
- [x] Identify primary keys and foreign-key-like columns
- [x] Define canonical entity mapping
- [x] Create SQLite database file
- [x] Build JSON -> SQLite loader
- [x] Prevent duplicate re-ingestion on startup
- [x] Verify table counts after load

## Phase 2 — Graph modeling
- [x] Define node and edge schema
- [x] Decide node types for orders, deliveries, invoices, payments, customers, products, addresses, and journal entries
- [x] Define relationship types
- [x] Build row-level graph from SQLite
- [x] Attach row metadata to graph nodes
- [x] Validate that key business flows are traceable

## Phase 3 — API layer
- [ ] Add graph fetch endpoint
- [ ] Add node expansion endpoint
- [ ] Add node metadata endpoint if needed
- [ ] Add chat/query endpoint
- [ ] Add domain guardrail for unrelated prompts
- [ ] Return highlighted node IDs from relevant answers

## Phase 4 — LLM pipeline
- [ ] Write schema-aware SQL generation prompt
- [ ] Write answer synthesis prompt
- [ ] Add SQL validation / retry on failure
- [ ] Ensure non-domain questions are rejected
- [ ] Ensure answers cite or reflect actual query results

## Phase 5 — Frontend
- [ ] Build graph canvas
- [ ] Show node metadata panel
- [ ] Support expand-on-click
- [ ] Build chat panel
- [ ] Sync chat results with graph highlights
- [ ] Keep layout clean and simple

## Phase 6 — Integration and testing
- [ ] Verify no manual setup is needed beyond env vars

## Phase 7 — Deployment
- [ ] Confirm backend runs from one command
- [ ] Confirm frontend points to backend via env var
- [ ] Deploy backend
- [ ] Deploy frontend
- [ ] Verify public demo works from a fresh environment


