export type Lead = {
  trade_name?: string
  entity_name?: string
  company?: string
  city?: string
  state?: string
  address?: string
  business_phone?: string
  website?: string
  general_email?: string
  instagram_url?: string
  facebook_url?: string
  google_rating?: string | number
  google_review_count?: string | number
  industry?: string
  formation_date?: string
  years_in_business?: string
  [key: string]: unknown
}

export type TaskStatus = 'in_progress' | 'completed' | 'failed'

export type EmailDraft = {
  name?: string
  email?: string
  subject_line?: string
  email_body?: string
}

/** One rendered row in the chat (persisted in Task.uiMessages) */
export type ChatMsg =
  | { id: string; kind: 'user'; text: string }
  | { id: string; kind: 'assistant'; text: string; typing?: boolean }
  | { id: string; kind: 'tool'; name: string; done: boolean }
  | { id: string; kind: 'leads'; leads: Lead[] }
  | { id: string; kind: 'report'; title: string }
  | { id: string; kind: 'emails'; drafts: EmailDraft[] }
  | { id: string; kind: 'stopped' }
  | { id: string; kind: 'error'; text: string }

export type Task = {
  id: number
  description: string
  status: TaskStatus
  outputs: string[]
  time: Date
  /** Legacy: kept empty for React UI */
  messages: { role: 'user' | 'assistant'; content: string }[]
  /** Full chat view state */
  uiMessages: ChatMsg[]
  /** For /api/chat history */
  apiHistory: { role: 'user' | 'assistant'; content: string }[]
}

export type ConfigResponse = {
  anthropic: boolean
  apollo: boolean
  hubspot: boolean
  gemini: boolean
  perplexity: boolean
  gmail: boolean
  crm: boolean
  google_places: boolean
  model_provider: string
  claude_model?: string
  gemini_model?: string
  perplexity_model?: string
  env_file_loaded?: boolean
  env_file_path?: string
  flask_app_dir?: string
}

export type SseEvent =
  | { type: 'text'; content: string }
  | { type: 'tool_start'; name: string }
  | { type: 'tool_end'; name: string; result?: { leads?: Lead[]; success?: boolean; count?: number; title?: string; drafts?: EmailDraft[] } }
  | { type: 'done' }
  | { type: 'error'; content: string }
