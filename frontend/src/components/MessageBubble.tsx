import type { ChatMessage } from '../store/graphStore'

type MessageBubbleProps = {
  message: ChatMessage
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const isAssistant = message.role === 'assistant'

  return (
    <article className={`message-bubble ${isAssistant ? 'assistant' : 'user'}`}>
      <div className="message-bubble__header">
        <div className={`message-avatar ${isAssistant ? 'assistant' : 'user'}`}>
          {isAssistant ? 'D' : 'U'}
        </div>
        <div>
          <p className="message-bubble__title">{isAssistant ? 'B-Graph' : 'You'}</p>
          <p className="message-bubble__subtitle">
            {isAssistant ? 'Graph Agent' : 'Dataset question'}
          </p>
        </div>
      </div>
      <p className="message-bubble__content">{message.content}</p>
    </article>
  )
}
