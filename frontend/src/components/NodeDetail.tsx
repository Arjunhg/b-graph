import type { NodeDetailPayload } from '../api/client'

type NodeDetailProps = {
  detail: NodeDetailPayload | null
}

function titleize(value: string) {
  return value
    .replaceAll('_', ' ')
    .replaceAll('-', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase())
}

export function NodeDetail({ detail }: NodeDetailProps) {
  if (!detail) {
    return null
  }

  const visibleEntries = Object.entries(detail.node.metadata).filter(([, value]) => value !== null && value !== '')
  const previewEntries = visibleEntries.slice(0, 12)
  const hiddenCount = Math.max(visibleEntries.length - previewEntries.length, 0)

  return (
    <aside className="node-detail-card">
      <div className="node-detail-card__heading">
        <div>
          <p className="node-detail-card__eyebrow">Entity</p>
          <h2>{titleize(detail.node.node_type)}</h2>
        </div>
        <span className="node-detail-card__badge">{detail.node.table}</span>
      </div>

      <div className="node-detail-card__stats">
        <span>Connections: {detail.degree}</span>
        <span>In: {detail.in_degree}</span>
        <span>Out: {detail.out_degree}</span>
      </div>

      <dl className="node-detail-card__list">
        {previewEntries.map(([key, value]) => (
          <div key={key} className="node-detail-card__row">
            <dt>{key}</dt>
            <dd>{String(value)}</dd>
          </div>
        ))}
      </dl>

      {hiddenCount > 0 ? (
        <p className="node-detail-card__note">
          Additional fields hidden for readability: {hiddenCount}
        </p>
      ) : null}
    </aside>
  )
}
