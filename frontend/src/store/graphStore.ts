import { create } from 'zustand'

import type {
  ChatPayload,
  GraphEdgeRecord,
  GraphNodeRecord,
  GraphPayload,
  NodeDetailPayload,
} from '../api/client'

export type ChatMessage = {
  id: string
  role: 'assistant' | 'user'
  content: string
}

type GraphState = {
  nodes: GraphNodeRecord[]
  edges: GraphEdgeRecord[]
  selectedNodeId: string | null
  selectedNodeDetail: NodeDetailPayload | null
  highlightedNodeIds: string[]
  tracedPath: string[]
  messages: ChatMessage[]
  isGraphLoading: boolean
  isChatLoading: boolean
  graphError: string | null
  chatError: string | null
  showGranularOverlay: boolean
  isCompactGraph: boolean
  setGraphLoading: (value: boolean) => void
  setChatLoading: (value: boolean) => void
  setGraphError: (value: string | null) => void
  setChatError: (value: string | null) => void
  replaceGraph: (payload: GraphPayload) => void
  mergeGraph: (payload: GraphPayload) => void
  setSelectedNodeDetail: (payload: NodeDetailPayload | null) => void
  setSelectedNodeId: (value: string | null) => void
  setHighlightedNodeIds: (value: string[]) => void
  setTracedPath: (value: string[]) => void
  pushMessage: (message: ChatMessage) => void
  applyChatResult: (payload: ChatPayload) => void
  toggleGranularOverlay: () => void
  toggleCompactGraph: () => void
}

const INITIAL_MESSAGE: ChatMessage = {
  id: 'assistant-welcome',
  role: 'assistant',
  content: 'Hi! I can help you analyze the Order to Cash process.',
}

function mergeUniqueNodes(existing: GraphNodeRecord[], incoming: GraphNodeRecord[]) {
  const byId = new Map(existing.map((node) => [node.id, node]))
  for (const node of incoming) {
    byId.set(node.id, node)
  }
  return Array.from(byId.values())
}

function mergeUniqueEdges(existing: GraphEdgeRecord[], incoming: GraphEdgeRecord[]) {
  const byId = new Map(existing.map((edge) => [edge.id, edge]))
  for (const edge of incoming) {
    byId.set(edge.id, edge)
  }
  return Array.from(byId.values())
}

export const useGraphStore = create<GraphState>((set) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  selectedNodeDetail: null,
  highlightedNodeIds: [],
  tracedPath: [],
  messages: [INITIAL_MESSAGE],
  isGraphLoading: false,
  isChatLoading: false,
  graphError: null,
  chatError: null,
  showGranularOverlay: true,
  isCompactGraph: false,
  setGraphLoading: (value) => set({ isGraphLoading: value }),
  setChatLoading: (value) => set({ isChatLoading: value }),
  setGraphError: (value) => set({ graphError: value }),
  setChatError: (value) => set({ chatError: value }),
  replaceGraph: (payload) =>
    set({
      nodes: payload.nodes,
      edges: payload.edges,
    }),
  mergeGraph: (payload) =>
    set((state) => ({
      nodes: mergeUniqueNodes(state.nodes, payload.nodes),
      edges: mergeUniqueEdges(state.edges, payload.edges),
    })),
  setSelectedNodeDetail: (payload) => set({ selectedNodeDetail: payload }),
  setSelectedNodeId: (value) => set({ selectedNodeId: value }),
  setHighlightedNodeIds: (value) => set({ highlightedNodeIds: value }),
  setTracedPath: (value) => set({ tracedPath: value }),
  pushMessage: (message) =>
    set((state) => ({
      messages: [...state.messages, message],
    })),
  applyChatResult: (payload) =>
    set((state) => ({
      messages: [
        ...state.messages,
        {
          id: `assistant-${Date.now()}`,
          role: 'assistant',
          content: payload.answer,
        },
      ],
      highlightedNodeIds: payload.highlighted_node_ids,
      tracedPath: payload.traced_path ?? [],
      chatError: null,
    })),
  toggleGranularOverlay: () =>
    set((state) => ({ showGranularOverlay: !state.showGranularOverlay })),
  toggleCompactGraph: () =>
    set((state) => ({ isCompactGraph: !state.isCompactGraph })),
}))
