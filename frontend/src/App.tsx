import { startTransition, useEffect, useEffectEvent } from 'react'

import {
  expandNode,
  fetchGraph,
  fetchNodeDetail,
  sendChatQuery,
  type GraphNodeRecord,
} from './api/client'
import { ChatPanel } from './components/ChatPanel'
import { GraphView } from './components/GraphView'
import { useGraphStore } from './store/graphStore'

function App() {
  const {
    nodes,
    edges,
    selectedNodeId,
    selectedNodeDetail,
    highlightedNodeIds,
    tracedPath,
    messages,
    isGraphLoading,
    isChatLoading,
    graphError,
    chatError,
    showGranularOverlay,
    isCompactGraph,
    replaceGraph,
    mergeGraph,
    setGraphLoading,
    setChatLoading,
    setGraphError,
    setChatError,
    setSelectedNodeId,
    setSelectedNodeDetail,
    setHighlightedNodeIds,
    pushMessage,
    applyChatResult,
    toggleGranularOverlay,
    toggleCompactGraph,
  } = useGraphStore()

  const loadInitialGraph = useEffectEvent(async () => {
    setGraphLoading(true)
    setGraphError(null)
    try {
      const payload = await fetchGraph()
      startTransition(() => {
        replaceGraph(payload)
      })
    } catch (error) {
      setGraphError(error instanceof Error ? error.message : 'Unable to load graph.')
    } finally {
      setGraphLoading(false)
    }
  })

  async function handleNodeSelect(node: GraphNodeRecord) {
    setSelectedNodeId(node.id)
    setGraphError(null)
    try {
      const [detail, expanded] = await Promise.all([
        fetchNodeDetail(node.id),
        expandNode(node.id),
      ])
      startTransition(() => {
        setSelectedNodeDetail(detail)
        mergeGraph(expanded)
        setHighlightedNodeIds([node.id])
      })
    } catch (error) {
      setGraphError(error instanceof Error ? error.message : 'Unable to expand node.')
    }
  }

  async function handleSend(query: string) {
    setChatLoading(true)
    setChatError(null)
    pushMessage({
      id: `user-${Date.now()}`,
      role: 'user',
      content: query,
    })
    try {
      const payload = await sendChatQuery(query)
      startTransition(() => {
        applyChatResult(payload)
      })
      return payload
    } catch (error) {
      setChatError(error instanceof Error ? error.message : 'Unable to send chat query.')
      throw error
    } finally {
      setChatLoading(false)
    }
  }

  useEffect(() => {
    void loadInitialGraph()
  }, [])

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="topbar__icon" aria-hidden="true">
          ||
        </div>
        <p className="topbar__crumbs">Mapping / Order to Cash</p>
      </header>

      <div className="workspace">
        <section className="workspace__main">
          <GraphView
            graph={{ nodes, edges }}
            highlightedNodeIds={highlightedNodeIds}
            tracedPath={tracedPath}
            selectedNodeId={selectedNodeId}
            selectedNodeDetail={selectedNodeDetail}
            showGranularOverlay={showGranularOverlay}
            isCompactGraph={isCompactGraph}
            onNodeSelect={(node) => {
              void handleNodeSelect(node)
            }}
            onDismiss={() => {
              setSelectedNodeId(null)
              setSelectedNodeDetail(null)
            }}
            onToggleGranularOverlay={toggleGranularOverlay}
            onToggleCompactGraph={toggleCompactGraph}
          />

          <div className="workspace__status">
            {isGraphLoading ? (
              <p>Loading graph canvas...</p>
            ) : graphError ? (
              <p className="workspace__error">{graphError}</p>
            ) : (
              <p>{nodes.length} nodes and {edges.length} relationships loaded.</p>
            )}
          </div>
        </section>

        <ChatPanel
          messages={messages}
          isChatLoading={isChatLoading}
          chatError={chatError}
          onSend={handleSend}
        />
      </div>
    </main>
  )
}

export default App
