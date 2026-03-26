import { useState, type FormEvent } from 'react'

import type { ChatPayload } from '../api/client'
import type { ChatMessage } from '../store/graphStore'
import { MessageBubble } from './MessageBubble'

type ChatPanelProps = {
  messages: ChatMessage[]
  isChatLoading: boolean
  chatError: string | null
  onSend: (query: string) => Promise<ChatPayload>
}

export function ChatPanel({ messages, isChatLoading, chatError, onSend }: ChatPanelProps) {
  const [draft, setDraft] = useState('')

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const query = draft.trim()
    if (!query || isChatLoading) {
      return
    }

    setDraft('')
    try {
      await onSend(query)
    } catch {
      setDraft(query)
    }
  }

  return (
    <aside className="chat-panel">
      <header className="chat-panel__header">
        <p className="chat-panel__eyebrow">Chat with Graph</p>
        <h2>Order to Cash</h2>
      </header>

      <div className="chat-panel__body">
        {messages.map((message) => (
          <MessageBubble key={message.id} message={message} />
        ))}
      </div>

      <footer className="chat-panel__composer">
        <div className="chat-panel__status">
          <span className="chat-panel__status-dot" />
          {isChatLoading ? 'B-Graph is analyzing the graph' : 'B-Graph is awaiting instructions'}
        </div>
        <form className="chat-panel__form" onSubmit={handleSubmit}>
          <textarea
            className="chat-panel__input"
            placeholder="Analyze anything"
            rows={3}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
          />
          <div className="chat-panel__actions">
            {chatError ? <p className="chat-panel__error">{chatError}</p> : <span />}
            <button className="chat-panel__send" type="submit" disabled={isChatLoading}>
              Send
            </button>
          </div>
        </form>
      </footer>
    </aside>
  )
}
