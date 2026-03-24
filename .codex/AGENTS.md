# B Graph Agent Instructions

You are building a graph-based Order-to-Cash exploration system from the provided dataset.

## Goal
Create a local-first app that:
- ingests JSON (JSONL-style) files into SQLite
- constructs a row-level business graph
- exposes graph exploration APIs
- supports natural-language querying through an LLM
- renders a graph UI with node inspection and expansion
- answers only dataset-related questions

## External libraries usage
When using any third-party library (e.g. react-force-graph):

- First, look up the official documentation or examples.
- Do not guess APIs or props.
- Prefer minimal working examples from official sources.
- Implement only what is needed for the current step.
- If unsure, choose the simplest documented approach.

Avoid:
- hallucinating props or methods
- overengineering beyond documented patterns

## Core constraints
- Use explicit business relationships; do not rely on embeddings for the core logic.
- Prefer deterministic SQL and graph traversal over semantic guessing.
- Restrict the assistant to the provided dataset and the Order-to-Cash domain.
- Refuse unrelated prompts with a fixed domain-only response.
- Keep the system easy to run locally with minimal setup.
- Avoid introducing external infrastructure that requires manual configuration from the reviewer.
- Always use uv add "dependency_name" to install any external package for python

## Data modeling rules
- Model actual business records as nodes.
- Model relationships between records as edges.
- Do not model tables as the primary graph unit.
- Preserve source row data as node metadata.
- Keep identifiers stable and traceable back to the raw CSV rows.

## Backend architecture
- Use FastAPI for the API layer.
- Use SQLite as the local source of truth.
- Build the graph from SQLite data.
- Expose:
  - graph fetch endpoints
  - graph expand endpoints
  - chat/query endpoints
- Run ingestion and graph construction on startup if needed.

## LLM behavior
- Use groq with model name: llama-3.3-70b-versatile
- Translate user questions into structured queries.
- Generate only valid SQLite-compatible SQL or a clearly defined graph traversal plan.
- Refuse non-domain questions.
- Ground all answers in query results.
- Return useful debugging metadata when appropriate, such as generated SQL and highlighted node IDs.

## UI behavior
- Always use pnpm to install or add new packages
- Provide a split view with graph and chat.
- For graph display use react-force-graph 2D version.
- Allow node expansion on click.
- Show metadata for selected nodes.
- Highlight graph elements referenced by the answer.

## Implementation discipline
- Build in phases.
- Keep each phase independently testable.
- Update the checklist file as steps are completed.
- Prefer simple, reliable implementations over over-engineered ones.
- Do not mark a phase complete until it is actually working end-to-end.