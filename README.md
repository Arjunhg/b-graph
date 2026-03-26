# B-Graph

B-Graph is a full-stack application designed to ingest fragmented, relational business data (like an SAP Order-to-Cash dataset), map it automatically into a SQLite database, and visualize the entire system as an interactive force-directed graph. 

It features an integrated LLM-powered chat interface that sits alongside the graph, allowing you to ask natural language questions ("How many orders do we have?", "Trace order 740570"). The system generates and runs SQL against the database, summarizes the business result in the chat, and visually highlights or animates the corresponding nodes and document flows directly on the canvas.

## Key Features

- **Automated SQLite Ingestion:** Parses raw JSONL data exports into a unified, relational database on startup (controlled via the `REINGEST_ON_STARTUP` env var).
- **Interactive Graph Visualization:** Renders thousands of nodes and relationships using a high-performance 2D force-directed canvas. Distinctly colors transactional documents (blue) versus master data like customers/products (pink).
- **LLM-Powered Chat Pipeline:** Uses the Groq API (`llama-3.3-70b-versatile`) to process natural language questions. It combines predefined deterministic SQL templates for known workflows (like tracing a document chain) with LLM-generated SQL fallbacks for ad-hoc requests.
- **Visual Path Tracing & Animation:** When you ask to trace a specific business document (e.g., "Trace order 740570"), the graph dynamically animates the exact chronological step-by-step document flow (Order → Delivery → Billing → Journal → Payment) using glowing amber nodes and flowing particles along the edges. Peripheral related data points are subtly highlighted in dark blue.

## Tech Stack

### Backend
- **Framework:** FastAPI
- **Database:** SQLite (built from flat JSONL files using `sqlite-utils`)
- **Graph Processing:** NetworkX
- **LLM Integration:** Groq API
- **Package Manager:** `uv`

### Frontend
- **Framework:** React + TypeScript + Vite
- **Graph Renderer:** `react-force-graph-2d`
- **State Management:** Zustand
- **Styling:** Vanilla CSS
- **Package Manager:** `pnpm`

---

## Local Development Setup

### 1. Prerequisites
- Python 3.11+ and the [`uv` package manager](https://docs.astral.sh/uv/)
- Node.js 18+ and `pnpm`
- A [Groq API Key](https://console.groq.com/keys)

### 2. Environment Configuration
Clone the repository and set up your `.env` file:
```bash
cp .env.example .env
```
Edit `.env` and configure your keys:
```env
# Required for the chat interface to process queries
GROQ_API_KEY="gsk_your_api_key_here"

# Model selection (default is fine)
GROQ_MODEL="llama-3.3-70b-versatile"

# On the very first run, set this to "true" to build the SQLite DB from the raw JSONL files.
# Once built, change it back to "false" to skip the ingestion step on future server restarts.
REINGEST_ON_STARTUP="true"
```

### 3. Start the Backend
```bash
# uv will automatically handle creating the virtual environment and installing dependencies
uv sync

# Start the FastAPI server (runs on port 8000)
uv run uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```
*Wait for the server to say `Application startup complete` before querying the app. If this is your first time starting up with `REINGEST_ON_STARTUP="true"`, building the database may take 30-60 seconds depending on data size.*

### 4. Start the Frontend
In a new terminal tab, navigate to the `frontend` directory:
```bash
cd frontend
pnpm install
pnpm run dev
```
The frontend will be available at `http://localhost:5173`. 

---