import axios from 'axios'

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL?.trim() || '',
})

export type GraphNodeRecord = {
  id: string
  node_type: string
  table: string
  label: string
  key: Record<string, string | number | boolean | null>
  metadata: Record<string, string | number | boolean | null>
}

export type GraphEdgeRecord = {
  id: string
  source: string
  target: string
  edge_type: string
  relationship: string
  metadata: Record<string, string | number | boolean | null>
}

export type GraphPayload = {
  nodes: GraphNodeRecord[]
  edges: GraphEdgeRecord[]
  total_nodes?: number
  total_edges?: number
  truncated?: boolean
  center_node_id?: string
  hops?: number
}

export type NodeDetailPayload = {
  node: GraphNodeRecord
  in_degree: number
  out_degree: number
  degree: number
}

export type ChatPayload = {
  answer: string
  highlighted_node_ids: string[]
  in_scope: boolean
  debug: {
    matched_keywords?: string[]
    matched_tokens?: string[]
    selected_tables?: string[]
    sql?: string | null
    row_count?: number
    columns?: string[]
    attempts?: Array<Record<string, unknown>>
    llm_enabled?: boolean
  }
}

export async function fetchGraph() {
  const response = await api.get<GraphPayload>('/api/graph', {
    params: { max_nodes: 1600, max_edges: 5000 },
  })
  return response.data
}

export async function expandNode(nodeId: string) {
  const response = await api.get<GraphPayload>(`/api/graph/expand/${encodeURIComponent(nodeId)}`, {
    params: { hops: 1, max_nodes: 220, max_edges: 900 },
  })
  return response.data
}

export async function fetchNodeDetail(nodeId: string) {
  const response = await api.get<NodeDetailPayload>(`/api/graph/node/${encodeURIComponent(nodeId)}`)
  return response.data
}

export async function sendChatQuery(query: string) {
  const response = await api.post<ChatPayload>('/api/chat', { query })
  return response.data
}
