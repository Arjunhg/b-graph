import { startTransition, useEffect, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'

import type { GraphEdgeRecord, GraphNodeRecord, GraphPayload } from '../api/client'
import { NodeDetail } from './NodeDetail'

type GraphViewProps = {
  graph: GraphPayload
  tracedPath: string[]           // ordered O2C path for step animation
  highlightedNodeIds: string[]
  selectedNodeId: string | null
  selectedNodeDetail: {
    node: GraphNodeRecord
    in_degree: number
    out_degree: number
    degree: number
  } | null
  showGranularOverlay: boolean
  isCompactGraph: boolean
  onNodeSelect: (node: GraphNodeRecord) => void
  onDismiss: () => void
  onToggleGranularOverlay: () => void
  onToggleCompactGraph: () => void
}

type ForceNode = GraphNodeRecord & {
  x?: number
  y?: number
  vx?: number
  vy?: number
  fx?: number
  fy?: number
}

type ForceEdge = GraphEdgeRecord & {
  source: string | ForceNode
  target: string | ForceNode
}

const OVERLAY_EDGE_TYPES = new Set([
  'PLANT_TO_STORAGE_LOCATION',
  'PRODUCT_TO_STORAGE_LOCATION',
  'PLANT_TO_PRODUCT_ASSIGNMENT',
  'PRODUCT_TO_PLANT_ASSIGNMENT',
])

function isOverlayEdge(edge: GraphEdgeRecord) {
  return OVERLAY_EDGE_TYPES.has(edge.edge_type)
}

function nodeBaseColor(node: GraphNodeRecord) {
  if (
    node.node_type.includes('customer') ||
    node.node_type.includes('product') ||
    node.node_type.includes('plant')
  ) {
    return '#f3a4b5'
  }
  return '#7db9f5'
}

function createVisibleGraph(graph: GraphPayload, showGranularOverlay: boolean) {
  if (showGranularOverlay) return graph

  const filteredEdges = graph.edges.filter((edge) => !isOverlayEdge(edge))
  const visibleNodeIds = new Set<string>()
  for (const edge of filteredEdges) {
    visibleNodeIds.add(edge.source)
    visibleNodeIds.add(edge.target)
  }
  const filteredNodes = graph.nodes.filter((node) => visibleNodeIds.has(node.id))
  return { nodes: filteredNodes, edges: filteredEdges }
}

export function GraphView({
  graph,
  tracedPath,
  highlightedNodeIds,
  selectedNodeId,
  selectedNodeDetail,
  showGranularOverlay,
  isCompactGraph,
  onNodeSelect,
  onDismiss,
  onToggleGranularOverlay,
  onToggleCompactGraph,
}: GraphViewProps) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const graphRef = useRef<any>(null)
  const stageRef = useRef<HTMLDivElement | null>(null)
  const [dimensions, setDimensions] = useState({ width: 900, height: 640 })
  // activeTrace holds the subset of tracedPath nodes revealed so far during animation
  const [activeTrace, setActiveTrace] = useState<Set<string>>(new Set())

  useEffect(() => {
    if (!stageRef.current) return
    const observer = new ResizeObserver(([entry]) => {
      const width = entry.contentRect.width
      const height = entry.contentRect.height
      startTransition(() => {
        setDimensions({
          width: Math.max(320, Math.floor(width)),
          height: Math.max(360, Math.floor(height)),
        })
      })
    })
    observer.observe(stageRef.current)
    return () => observer.disconnect()
  }, [])

  const visibleGraph = createVisibleGraph(graph, showGranularOverlay)
  const highlightedSet = new Set(highlightedNodeIds)

  // zoomToFit when graph changes
  useEffect(() => {
    if (!graphRef.current || visibleGraph.nodes.length === 0) return
    const timer = window.setTimeout(() => {
      graphRef.current?.zoomToFit(500, 56)
    }, 180)
    return () => window.clearTimeout(timer)
  }, [visibleGraph.nodes.length, visibleGraph.edges.length, isCompactGraph, showGranularOverlay])

  // Step-by-step path animation: reveal one node every 350ms
  useEffect(() => {
    setActiveTrace(new Set())
    if (!tracedPath.length) return
    let step = 0
    const interval = window.setInterval(() => {
      step += 1
      setActiveTrace(new Set(tracedPath.slice(0, step)))
      if (step >= tracedPath.length) window.clearInterval(interval)
    }, 350)
    return () => window.clearInterval(interval)
  }, [tracedPath])

  return (
    <section className="graph-stage-shell">
      <div className="graph-stage__toolbar">
        <button className="graph-toolbar-button" type="button" onClick={onToggleCompactGraph}>
          {isCompactGraph ? 'Expand' : 'Minimize'}
        </button>
        <button
          className={`graph-toolbar-button graph-toolbar-button--dark ${showGranularOverlay ? '' : 'is-muted'}`}
          type="button"
          onClick={onToggleGranularOverlay}
        >
          {showGranularOverlay ? 'Hide Granular Overlay' : 'Show Granular Overlay'}
        </button>
      </div>

      <div className="graph-stage" ref={stageRef}>
        <ForceGraph2D
          ref={graphRef}
          width={dimensions.width}
          height={dimensions.height}
          graphData={{
            nodes: visibleGraph.nodes as ForceNode[],
            links: visibleGraph.edges as ForceEdge[],
          }}
          backgroundColor="rgba(255,255,255,0)"
          nodeRelSize={isCompactGraph ? 2.2 : 3.6}
          cooldownTicks={140}
          linkWidth={(edge) => {
            // After force simulation, source/target become node objects — extract id from either
            const src = typeof edge.source === 'object' ? (edge.source as ForceNode).id : edge.source as string
            const tgt = typeof edge.target === 'object' ? (edge.target as ForceNode).id : edge.target as string
            if (activeTrace.has(src) && activeTrace.has(tgt)) return 2.4
            if (highlightedSet.has(src) || highlightedSet.has(tgt)) return 1.8
            const record = edge as GraphEdgeRecord
            return isOverlayEdge({ ...record, source: src, target: tgt }) ? 0.35 : 0.9
          }}
          linkDirectionalParticles={(edge) => {
            const src = typeof edge.source === 'object' ? (edge.source as ForceNode).id : edge.source as string
            const tgt = typeof edge.target === 'object' ? (edge.target as ForceNode).id : edge.target as string
            return activeTrace.has(src) && activeTrace.has(tgt) ? 3 : 0
          }}
          linkDirectionalParticleWidth={2.5}
          linkDirectionalParticleColor={() => 'rgba(251, 146, 60, 0.9)'}
          linkColor={(edge) => {
            const src = typeof edge.source === 'object' ? (edge.source as ForceNode).id : edge.source as string
            const tgt = typeof edge.target === 'object' ? (edge.target as ForceNode).id : edge.target as string
            if (activeTrace.has(src) && activeTrace.has(tgt)) return 'rgba(251, 146, 60, 0.75)'
            const isHighlighted = highlightedSet.has(src) || highlightedSet.has(tgt)
            if (isHighlighted) return 'rgba(3, 105, 161, 0.65)'
            const record = edge as GraphEdgeRecord
            return isOverlayEdge({ ...record, source: src, target: tgt }) ? 'rgba(125, 185, 245, 0.08)' : 'rgba(125, 185, 245, 0.26)'
          }}
          nodeCanvasObject={(node, context, globalScale) => {
            const label = node.id
            const radius = isCompactGraph ? 2.1 : 3
            const isTraced = activeTrace.has(node.id)
            const isHighlighted = highlightedSet.has(node.id)
            const isSelected = node.id === selectedNodeId

            // Color priority: selected > traced (amber) > highlighted (dark) > base color
            let color: string
            if (isSelected) color = '#0369a1'
            else if (isTraced) color = '#f97316'
            else if (isHighlighted) color = '#111827'
            else color = nodeBaseColor(node)

            context.beginPath()
            context.arc(node.x ?? 0, node.y ?? 0, radius, 0, 2 * Math.PI, false)
            context.fillStyle = color
            context.fill()

            // Amber glow ring for traced path nodes
            if (isTraced) {
              context.beginPath()
              context.arc(node.x ?? 0, node.y ?? 0, radius + 3.5, 0, 2 * Math.PI, false)
              context.strokeStyle = 'rgba(251, 146, 60, 0.35)'
              context.lineWidth = 2.5
              context.stroke()
            }

            // Blue ring for selected node
            if (isSelected) {
              context.beginPath()
              context.arc(node.x ?? 0, node.y ?? 0, radius + 4, 0, 2 * Math.PI, false)
              context.strokeStyle = 'rgba(3, 105, 161, 0.3)'
              context.lineWidth = 2
              context.stroke()
            }

            // Show label for any prominent node
            if (isTraced || isHighlighted || isSelected) {
              const fontSize = 12 / globalScale
              context.font = `600 ${fontSize}px ui-sans-serif, system-ui, sans-serif`
              context.fillStyle = '#1f2937'
              context.fillText(label.split(':').slice(-1)[0], (node.x ?? 0) + 7, (node.y ?? 0) + 3)
            }
          }}
          onNodeClick={(node) => onNodeSelect(node)}
          onBackgroundClick={() => onDismiss()}
          enablePanInteraction
          enableZoomInteraction
          showPointerCursor
          autoPauseRedraw
        />

        <div className="graph-stage__fade graph-stage__fade--top" />
        <div className="graph-stage__fade graph-stage__fade--right" />
        <NodeDetail detail={selectedNodeDetail} />
      </div>
    </section>
  )
}
