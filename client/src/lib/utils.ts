import { marked } from 'marked'

export function escHtml(str: string): string {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

export function renderMarkdown(text: string): string {
  try {
    return marked.parse(text, { async: false, breaks: true, gfm: true } as { async?: false }) as string
  } catch {
    return text.replace(/\n/g, '<br />')
  }
}

export function timeAgo(date: Date): string {
  const secs = Math.floor((Date.now() - date.getTime()) / 1000)
  if (secs < 60) return 'Just now'
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`
  return date.toLocaleDateString()
}

export const fmtMs = (ms: number) =>
  ms >= 60000 ? `${(ms / 60000).toFixed(1)}m` : ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`
export const fmtNum = (n: number) => Number(n).toLocaleString()
export const fmtCost = (c: number) => (c < 0.0001 ? '<$0.0001' : `$${Number(c).toFixed(4)}`)

export const PROVIDER_COLORS: Record<string, { bg: string; text: string; border: string; dot: string }> = {
  anthropic: { bg: 'bg-purple-50', text: 'text-purple-700', border: 'border-purple-200', dot: 'bg-purple-500' },
  gemini: { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-200', dot: 'bg-blue-500' },
  perplexity: { bg: 'bg-teal-50', text: 'text-teal-700', border: 'border-teal-200', dot: 'bg-teal-500' },
}
export const PROVIDER_ICONS: Record<string, string> = { anthropic: '🤖', gemini: '✨', perplexity: '🔍' }

const TOOL_ICONS: Record<string, string> = {
  apollo_search_people: '🔍',
  apollo_enrich_existing_leads: '✨',
  hubspot_create_contact: '📥',
  upload_leads_to_hubspot: '📥',
  get_collected_leads: '📋',
  web_search: '🌐',
  save_outreach_csv: '💾',
  save_leads_csv: '💾',
  save_research_report: '📄',
  enrich_leads_batch: '⚡',
  sunbiz_lookup: '🏛',
  scrape_website_contact: '🌐',
  get_google_reviews: '⭐',
  search_businesses_maps: '🗺️',
  send_gmail_email: '📤',
  create_gmail_drafts: '📧',
  query_tenant_crm: '📊',
}

export function getToolIcon(name: string): string {
  return TOOL_ICONS[name] || '⚙️'
}

export function genId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`
}
