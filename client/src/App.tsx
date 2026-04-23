import { useCallback, useEffect, useRef, useState } from 'react'
import { LeadsTableBlock } from './components/LeadsTableBlock'
import { EmailDraftsBlock } from './components/EmailDraftsBlock'
import { ResearchReportBlock } from './components/ResearchReportBlock'
import { SettingsModal } from './components/SettingsModal'
import type { ChatMsg, SseEvent, Task, TaskStatus } from './types'
import {
  escHtml,
  fmtCost,
  fmtMs,
  fmtNum,
  genId,
  getToolIcon,
  renderMarkdown,
  timeAgo,
  PROVIDER_COLORS,
  PROVIDER_ICONS,
} from './lib/utils'

type Page = 'tasks' | 'session' | 'connectors' | 'performance'

const LEAD_TOOL_NAMES = new Set([
  'apollo_search_people',
  'apollo_enrich_existing_leads',
  'enrich_leads_batch',
  'get_collected_leads',
  'query_tenant_crm',
])

function parseSseEvent(line: string): SseEvent | null {
  if (!line.startsWith('data: ')) return null
  try {
    return JSON.parse(line.slice(6)) as SseEvent
  } catch {
    return null
  }
}

function ConnectorsGrid({ onOpenSettings }: { onOpenSettings: () => void }) {
  const [config, setConfig] = useState<{
    anthropic: boolean
    hubspot: boolean
    apollo: boolean
    gmail: boolean
    crm: boolean
    google_places: boolean
  } | null>(null)

  useEffect(() => {
    fetch('/api/config')
      .then((r) => r.json())
      .then(setConfig)
      .catch(() => setConfig(null))
  }, [])

  if (!config) {
    return <p className="text-sm text-gray-400">Loading…</p>
  }

  const list = [
    { key: 'anthropic' as const, name: 'Anthropic Claude', icon: 'fa-solid fa-robot', color: 'text-purple-600', desc: 'AI model powering the agent. Required for all operations.', ok: config.anthropic },
    { key: 'hubspot' as const, name: 'HubSpot', icon: 'fa-brands fa-hubspot', color: 'text-orange-500', desc: 'CRM platform. Sync contacts and companies automatically.', ok: config.hubspot },
    { key: 'apollo' as const, name: 'Apollo.io', icon: 'fa-solid fa-magnifying-glass', color: 'text-gray-700', desc: 'B2B lead database. Search companies and contacts.', ok: config.apollo },
    { key: 'gmail' as const, name: 'Gmail', icon: 'fa-brands fa-google', color: 'text-red-500', desc: 'Send outreach emails directly from your Gmail account.', ok: config.gmail },
    { key: 'crm' as const, name: 'Business Leads DB', icon: 'fa-solid fa-table', color: 'text-green-600', desc: 'Local Excel database of Miami-Dade & Broward businesses.', ok: config.crm },
  ]

  return (
    <div className="grid grid-cols-2 gap-4 max-w-3xl">
      {list.map((c) => (
        <div key={c.key} className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 rounded-lg bg-gray-50 border border-gray-100 flex items-center justify-center">
              <i className={`${c.icon} ${c.color} text-xl`} />
            </div>
            <div>
              <div className="font-semibold text-gray-900 text-sm">{c.name}</div>
              <span className="text-xs text-teal-600 font-medium flex items-center gap-1">
                {c.ok ? (
                  <>
                    <span className="w-1.5 h-1.5 rounded-full bg-teal-500 inline-block" /> Connected
                  </>
                ) : (
                  <span className="text-gray-400">Not connected</span>
                )}
              </span>
            </div>
          </div>
          <p className="text-xs text-gray-500 mb-4">{c.desc}</p>
          {c.key === 'gmail' ? (
            <a href="/api/gmail/auth" className="w-full text-center text-xs font-medium py-1.5 rounded-lg border border-gray-200 text-gray-600 bg-gray-50 hover:bg-gray-100 transition-colors block">
              Connect with Google
            </a>
          ) : (
            <button
              type="button"
              onClick={onOpenSettings}
              className="w-full text-center text-xs font-medium py-1.5 rounded-lg border border-gray-200 text-gray-600 bg-gray-50 hover:bg-gray-100"
            >
              Open settings
            </button>
          )}
        </div>
      ))}
      {config.google_places && (
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm col-span-2">
          <div className="text-sm font-medium text-gray-800">Google Places API</div>
          <p className="text-xs text-teal-600 mt-1">Configured on server</p>
        </div>
      )}
    </div>
  )
}

function PerformanceView() {
  const [data, setData] = useState<{
    summary: Record<string, { requests: number; success_rate: number; avg_ms: number; avg_tokens: number; total_leads: number; avg_leads: number; avg_cost_usd: number; total_cost_usd: number }>
    recent: Array<{
      ts: number
      provider: string
      model: string
      duration_ms: number
      input_tokens: number
      output_tokens: number
      tool_calls: number
      leads_found: number
      cost_usd: number
      success: boolean
    }>
  } | null>(null)

  const load = useCallback(() => {
    fetch('/api/performance')
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  if (!data) {
    return <p className="text-gray-400 text-sm p-8">Loading…</p>
  }
  const { summary, recent } = data
  const providers = Object.keys(summary)
  const totalReqs = providers.reduce((s, p) => s + summary[p].requests, 0)
  const totalLeads = providers.reduce((s, p) => s + summary[p].total_leads, 0)
  const totalCost = providers.reduce((s, p) => s + summary[p].total_cost_usd, 0)
  const overallAvgMs = providers.length
    ? Math.round(providers.reduce((s, p) => s + summary[p].avg_ms, 0) / providers.length)
    : 0

  return (
    <div className="flex-1 overflow-y-auto p-8 space-y-6">
      <div className="flex justify-end">
        <button type="button" onClick={load} className="inline-flex items-center gap-2 text-xs text-teal-600 hover:text-teal-800 font-medium">
          <i className="fa-solid fa-arrows-rotate" /> Refresh
        </button>
      </div>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
          <p className="text-xs text-gray-400 mb-1">Total Requests</p>
          <p className="text-2xl font-bold text-gray-900">{fmtNum(totalReqs)}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
          <p className="text-xs text-gray-400 mb-1">Avg Response Time</p>
          <p className="text-2xl font-bold text-gray-900">{fmtMs(overallAvgMs)}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
          <p className="text-xs text-gray-400 mb-1">Total Leads Found</p>
          <p className="text-2xl font-bold text-gray-900">{fmtNum(totalLeads)}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
          <p className="text-xs text-gray-400 mb-1">Est. Total Cost</p>
          <p className="text-2xl font-bold text-gray-900">{fmtCost(totalCost)}</p>
        </div>
      </div>
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
        <div className="px-6 py-3 bg-gray-50 border-b border-gray-100">
          <h2 className="text-sm font-semibold text-gray-700">Provider Comparison</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="text-[11px] font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-5 py-3">Provider</th>
                <th className="px-5 py-3">Requests</th>
                <th className="px-5 py-3">Success</th>
                <th className="px-5 py-3">Avg Time</th>
                <th className="px-5 py-3">Avg Tokens</th>
                <th className="px-5 py-3">Leads Found</th>
                <th className="px-5 py-3">Avg Cost</th>
                <th className="px-5 py-3">Total Cost</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {providers.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-5 py-8 text-center text-gray-400 text-xs">
                    No data yet — run some queries first.
                  </td>
                </tr>
              ) : (
                providers.map((p) => {
                  const d = summary[p]
                  const c = PROVIDER_COLORS[p] || PROVIDER_COLORS.anthropic
                  const icon = PROVIDER_ICONS[p] || '⚙️'
                  const successColor =
                    d.success_rate >= 90 ? 'text-green-600' : d.success_rate >= 70 ? 'text-amber-600' : 'text-red-500'
                  return (
                    <tr key={p} className="hover:bg-gray-50">
                      <td className="px-5 py-3">
                        <span
                          className={`inline-flex items-center gap-2 ${c.bg} ${c.text} border ${c.border} px-2.5 py-1 rounded-full text-xs font-semibold`}
                        >
                          <span className={`w-1.5 h-1.5 rounded-full ${c.dot} inline-block`} />
                          {icon} {p.charAt(0).toUpperCase() + p.slice(1)}
                        </span>
                      </td>
                      <td className="px-5 py-3 text-gray-700 font-medium">{fmtNum(d.requests)}</td>
                      <td className={`px-5 py-3 font-semibold ${successColor}`}>{d.success_rate}%</td>
                      <td className="px-5 py-3 text-gray-700">{fmtMs(d.avg_ms)}</td>
                      <td className="px-5 py-3 text-gray-700">{fmtNum(d.avg_tokens)}</td>
                      <td className="px-5 py-3 text-gray-700">
                        {fmtNum(d.total_leads)} <span className="text-gray-400 text-xs">(avg {d.avg_leads})</span>
                      </td>
                      <td className="px-5 py-3 text-gray-700">{fmtCost(d.avg_cost_usd)}</td>
                      <td className="px-5 py-3 font-semibold text-gray-800">{fmtCost(d.total_cost_usd)}</td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
        <div className="px-6 py-3 bg-gray-50 border-b border-gray-100">
          <h2 className="text-sm font-semibold text-gray-700">Recent Requests</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="text-[11px] font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-5 py-3">Time</th>
                <th className="px-5 py-3">Provider</th>
                <th className="px-5 py-3">Model</th>
                <th className="px-5 py-3">Duration</th>
                <th className="px-5 py-3">Tokens</th>
                <th className="px-5 py-3">Tools</th>
                <th className="px-5 py-3">Leads</th>
                <th className="px-5 py-3">Cost</th>
                <th className="px-5 py-3">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {!recent?.length ? (
                <tr>
                  <td colSpan={9} className="px-5 py-8 text-center text-gray-400 text-xs">
                    No data yet.
                  </td>
                </tr>
              ) : (
                recent.map((r, i) => {
                  const c = PROVIDER_COLORS[r.provider] || PROVIDER_COLORS.anthropic
                  const icon = PROVIDER_ICONS[r.provider] || '⚙️'
                  const d = new Date(r.ts * 1000)
                  const timeStr = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
                  return (
                    <tr key={i} className="hover:bg-gray-50 text-xs">
                      <td className="px-5 py-2.5 text-gray-400">{timeStr}</td>
                      <td className="px-5 py-2.5">
                        <span
                          className={`inline-flex items-center gap-1 ${c.bg} ${c.text} border ${c.border} px-2 py-0.5 rounded-full font-medium`}
                        >
                          {icon} {r.provider}
                        </span>
                      </td>
                      <td className="px-5 py-2.5 text-gray-500 font-mono">{escHtml(r.model)}</td>
                      <td className="px-5 py-2.5 text-gray-700 font-medium">{fmtMs(r.duration_ms)}</td>
                      <td className="px-5 py-2.5 text-gray-600">{fmtNum(r.input_tokens + r.output_tokens)}</td>
                      <td className="px-5 py-2.5 text-gray-600">{r.tool_calls}</td>
                      <td className="px-5 py-2.5 text-gray-600">{r.leads_found}</td>
                      <td className="px-5 py-2.5 text-gray-600">{fmtCost(r.cost_usd)}</td>
                      <td className="px-5 py-2.5">
                        {r.success ? (
                          <span className="text-green-600 font-semibold">✓ OK</span>
                        ) : (
                          <span className="text-red-500 font-semibold">✗ Error</span>
                        )}
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function MessageRow({
  m,
  onSendMessage,
}: {
  m: ChatMsg
  onSendMessage: (t: string) => void
}) {
  if (m.kind === 'user') {
    return (
      <div className="flex justify-end">
        <div className="bg-[#0f172a] text-white rounded-2xl rounded-tr-sm px-5 py-3 max-w-lg text-sm leading-relaxed">
          {m.text}
        </div>
      </div>
    )
  }
  if (m.kind === 'assistant') {
    return (
      <div className="flex items-start space-x-4">
        <div className="bg-teal-500 text-white rounded-lg h-8 w-8 flex items-center justify-center flex-shrink-0 mt-1 text-xs font-bold">M</div>
        <div className="flex-1 min-w-0">
          <p className="text-xs font-semibold text-gray-400 mb-2">MMG Agent</p>
          <div
            className={`bg-white border border-gray-200 rounded-xl px-5 py-4 text-sm text-gray-700 leading-relaxed shadow-sm agent-content ${
              m.typing ? 'typing-cursor' : ''
            }`}
            dangerouslySetInnerHTML={{ __html: renderMarkdown(m.text) }}
          />
        </div>
      </div>
    )
  }
  if (m.kind === 'tool') {
    return (
      <div className="flex items-start space-x-4">
        <div className="w-8 flex-shrink-0" />
        <span
          className={`${
            m.done
              ? 'bg-green-50 text-green-700 border-green-200'
              : 'bg-blue-50 text-blue-700 border-blue-200 tool-running'
          } border rounded-full px-3 py-1 text-xs font-medium inline-flex items-center gap-1.5`}
        >
          <span>{getToolIcon(m.name)}</span>
          <span>{m.done ? '✓' : '…'}</span>
          <span>{m.name.replace(/_/g, ' ')}</span>
        </span>
      </div>
    )
  }
  if (m.kind === 'leads') {
    return <LeadsTableBlock leads={m.leads} onSendMessage={onSendMessage} />
  }
  if (m.kind === 'report') {
    return <ResearchReportBlock title={m.title} />
  }
  if (m.kind === 'emails') {
    return <EmailDraftsBlock initialDrafts={m.drafts} />
  }
  if (m.kind === 'stopped') {
    return (
      <div className="text-xs text-gray-400 text-center py-1" />
    )
  }
  if (m.kind === 'error') {
    return (
      <div className="text-sm text-red-500">
        Error: {m.text}
      </div>
    )
  }
  return null
}

export default function App() {
  const [page, setPage] = useState<Page>('tasks')
  const [tasks, setTasks] = useState<Task[]>([])
  const [currentTaskId, setCurrentTaskId] = useState<number | null>(null)
  const [messages, setMessages] = useState<ChatMsg[]>([])
  const [chatHistory, setChatHistory] = useState<{ role: 'user' | 'assistant'; content: string }[]>([])
  const chatHistoryRef = useRef(chatHistory)
  useEffect(() => {
    chatHistoryRef.current = chatHistory
  }, [chatHistory])

  const [sessionTitle, setSessionTitle] = useState('Agent Session')
  const [isStreaming, setIsStreaming] = useState(false)
  const [hasLeads, setHasLeads] = useState(false)
  const [hasOutreach, setHasOutreach] = useState(false)
  const [toastMsg, setToastMsg] = useState('')
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [input, setInput] = useState('')

  const activeReader = useRef<ReadableStreamDefaultReader<Uint8Array> | null>(null)
  const assistantAcc = useRef('')
  const assistantRowId = useRef<string | null>(null)
  const toolRowIds = useRef<Record<string, string>>({})

  const showToast = useCallback((msg: string) => {
    setToastMsg(msg)
    setTimeout(() => setToastMsg(''), 2500)
  }, [])

  const newTask = useCallback(() => {
    const tid = Date.now()
    setTasks((prev) => {
      const task: Task = {
        id: tid,
        description: 'New task',
        status: 'in_progress' as TaskStatus,
        outputs: [],
        time: new Date(),
        messages: [],
        uiMessages: [],
        apiHistory: [],
      }
      return [task, ...prev]
    })
    setCurrentTaskId(tid)
    setMessages([])
    setChatHistory([])
    setHasLeads(false)
    setHasOutreach(false)
    setSessionTitle('Agent Session')
    setPage('session')
    void fetch('/api/clear_leads', { method: 'POST' })
  }, [])

  const openTask = (id: number) => {
    const task = tasks.find((t) => t.id === id)
    if (!task) return
    setCurrentTaskId(id)
    setMessages(task.uiMessages || [])
    setChatHistory(task.apiHistory || [])
    setHasLeads(!!(task.uiMessages || []).some((m) => m.kind === 'leads'))
    setHasOutreach(task.outputs.some((o) => o.includes('email draft')))
    setSessionTitle(task.description !== 'New task' ? task.description.slice(0, 40) : 'Agent Session')
    setPage('session')
  }

  // Load default model provider
  useEffect(() => {
    fetch('/api/config')
      .then((r) => r.json())
      .catch(() => ({}))
  }, [])

  const stopStreaming = useCallback(() => {
    if (activeReader.current) {
      void activeReader.current.cancel()
      activeReader.current = null
    }
    setIsStreaming(false)
    setMessages((m) => [...m, { id: genId(), kind: 'stopped' }])
  }, [])

  const sendMessage = useCallback(
    async (prefill?: string) => {
      if (isStreaming) return
      const text = (prefill ?? input).trim()
      if (!text) return
      setInput('')
      setIsStreaming(true)
      assistantAcc.current = ''
      assistantRowId.current = null
      toolRowIds.current = {}

      const task = tasks.find((t) => t.id === currentTaskId)
      if (task && task.description === 'New task') {
        const desc = text.length > 60 ? text.slice(0, 60) + '…' : text
        setTasks((prev) => prev.map((t) => (t.id === currentTaskId ? { ...t, description: desc } : t)))
        setSessionTitle(desc.slice(0, 40))
      }

      setMessages((m) => [...m, { id: genId(), kind: 'user', text: text }])

      const historyPayload = chatHistoryRef.current

      try {
        const resp = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: text, history: historyPayload }),
        })
        if (!resp.ok) {
          const errText = await resp.text()
          setMessages((m) => [...m, { id: genId(), kind: 'error', text: `Server ${resp.status}: ${errText}` }])
          setIsStreaming(false)
          return
        }
        const reader = resp.body?.getReader()
        if (!reader) {
          setIsStreaming(false)
          return
        }
        activeReader.current = reader
        const decoder = new TextDecoder()
        let buffer = ''

        const updateAssistant = (chunk: string) => {
          assistantAcc.current += chunk
          if (!assistantRowId.current) {
            const id = genId()
            assistantRowId.current = id
            setMessages((m) => [...m, { id, kind: 'assistant', text: assistantAcc.current, typing: true }])
          } else {
            const aid = assistantRowId.current
            setMessages((m) =>
              m.map((x) => (x.id === aid && x.kind === 'assistant' ? { ...x, text: assistantAcc.current, typing: true } : x)),
            )
          }
        }

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''
          for (const line of lines) {
            const evt = parseSseEvent(line)
            if (!evt) continue
            if (evt.type === 'text') {
              updateAssistant(evt.content)
            } else if (evt.type === 'tool_start') {
              const id = genId()
              toolRowIds.current[evt.name] = id
              setMessages((m) => [...m, { id, kind: 'tool', name: evt.name, done: false }])
              if (evt.name === 'search_businesses_maps') {
                if (!assistantRowId.current) {
                  const newId = genId()
                  assistantRowId.current = newId
                  setMessages((m) => [
                    ...m,
                    { id: newId, kind: 'assistant', text: '_🗺️ Searching Google Maps…_', typing: true },
                  ])
                } else {
                  setMessages((m) =>
                    m.map((x) =>
                      x.id === assistantRowId.current && x.kind === 'assistant'
                        ? { ...x, text: '_🗺️ Searching Google Maps…_', typing: true }
                        : x,
                    ),
                  )
                }
              } else if (evt.name === 'enrich_leads_batch') {
                if (!assistantRowId.current) {
                  const newId = genId()
                  assistantRowId.current = newId
                  setMessages((m) => [
                    ...m,
                    { id: newId, kind: 'assistant', text: '_⚡ Enriching leads with Sunbiz, website, and contact data…_', typing: true },
                  ])
                } else {
                  setMessages((m) =>
                    m.map((x) =>
                      x.id === assistantRowId.current && x.kind === 'assistant'
                        ? { ...x, text: '_⚡ Enriching leads with Sunbiz, website, and contact data…_', typing: true }
                        : x,
                    ),
                  )
                }
              }
            } else if (evt.type === 'tool_end') {
              const tname = evt.name
              const tid = toolRowIds.current[tname]
              if (tid) {
                setMessages((m) => m.map((x) => (x.id === tid && x.kind === 'tool' ? { ...x, done: true } : x)))
              }
              const res = evt.result
              if (res?.leads?.length && LEAD_TOOL_NAMES.has(tname)) {
                setMessages((m) => [...m, { id: genId(), kind: 'leads', leads: res.leads! }])
                setHasLeads(true)
                if (currentTaskId) {
                  setTasks((prev) =>
                    prev.map((t) => {
                      if (t.id !== currentTaskId) return t
                      const outs = t.outputs.filter((o) => !o.includes('prospect'))
                      outs.push(`${res.leads!.length} prospects`)
                      return { ...t, outputs: outs }
                    }),
                  )
                }
              }
              if (tname === 'save_research_report' && res?.success) {
                setMessages((m) => [
                  ...m,
                  { id: genId(), kind: 'report', title: (res as { title?: string }).title || 'Research Report' },
                ])
                if (currentTaskId) {
                  setTasks((prev) =>
                    prev.map((t) => {
                      if (t.id !== currentTaskId) return t
                      const outs = t.outputs.filter((o) => !o.includes('research report'))
                      outs.push('research report')
                      return { ...t, outputs: outs }
                    }),
                  )
                }
              }
              if (tname === 'save_outreach_csv' && res?.success) {
                setHasOutreach(true)
                if (res.drafts?.length) {
                  setMessages((m) => [...m, { id: genId(), kind: 'emails', drafts: res.drafts! }])
                }
                if (currentTaskId) {
                  setTasks((prev) =>
                    prev.map((t) => {
                      if (t.id !== currentTaskId) return t
                      const outs = t.outputs.filter((o) => !o.includes('email draft'))
                      outs.push(`${(res as { count?: number }).count ?? res.drafts?.length ?? 0} email drafts`)
                      return { ...t, outputs: outs }
                    }),
                  )
                }
              }
              if (tname === 'hubspot_create_contact' && (res as { success?: boolean })?.success) {
                if (currentTaskId) {
                  setTasks((prev) =>
                    prev.map((t) => {
                      if (t.id !== currentTaskId) return t
                      const ex = t.outputs.find((o) => o.includes('HubSpot'))
                      if (ex) {
                        const n = (parseInt(ex, 10) || 0) + 1
                        return { ...t, outputs: [...t.outputs.filter((o) => !o.includes('HubSpot')), `${n} HubSpot contacts`] }
                      }
                      return { ...t, outputs: [...t.outputs, '1 HubSpot contact'] }
                    }),
                  )
                }
              }
            } else if (evt.type === 'done') {
              const aid = assistantRowId.current
              if (aid) {
                setMessages((m) =>
                  m.map((x) => (x.id === aid && x.kind === 'assistant' ? { ...x, typing: false } : x)),
                )
              }
              const finalText = assistantAcc.current
              setChatHistory((h) => {
                const next = [
                  ...h,
                  { role: 'user' as const, content: text },
                  { role: 'assistant' as const, content: finalText },
                ]
                chatHistoryRef.current = next
                return next
              })
              if (currentTaskId) {
                setTasks((prev) => prev.map((t) => (t.id === currentTaskId ? { ...t, status: 'completed' } : t)))
              }
            } else if (evt.type === 'error') {
              setMessages((m) => {
                const aid = assistantRowId.current
                if (!aid) return [...m, { id: genId(), kind: 'error', text: evt.content }]
                return m.map((x) =>
                  x.id === aid && x.kind === 'assistant'
                    ? { ...x, text: x.text + `<p class="text-red-500 mt-2 text-xs">Error: ${escHtml(evt.content)}</p>`, typing: false }
                    : x,
                )
              })
              if (currentTaskId) {
                setTasks((prev) => prev.map((t) => (t.id === currentTaskId ? { ...t, status: 'failed' } : t)))
              }
            }
          }
        }
      } catch (e) {
        setMessages((m) => [...m, { id: genId(), kind: 'error', text: (e as Error).message }])
        if (currentTaskId) {
          setTasks((prev) => prev.map((t) => (t.id === currentTaskId ? { ...t, status: 'failed' } : t)))
        }
      } finally {
        activeReader.current = null
        assistantRowId.current = null
        setIsStreaming(false)
      }
    },
    [currentTaskId, input, isStreaming, tasks],
  )

  useEffect(() => {
    if (currentTaskId == null) return
    setTasks((prev) => prev.map((t) => (t.id === currentTaskId ? { ...t, uiMessages: messages, apiHistory: chatHistory } : t)))
  }, [messages, chatHistory, currentTaskId])

  const nav = (p: Page) => {
    setPage(p)
    if (p === 'performance' || p === 'connectors') {
      // optional refresh
    }
  }

  return (
    <div className="flex h-screen bg-white text-gray-800 overflow-hidden">
      <aside className="w-64 bg-[#0f172a] text-gray-400 flex flex-col justify-between flex-shrink-0">
        <div>
          <div className="h-16 flex items-center px-6 border-b border-gray-800">
            <div className="bg-teal-500 text-white rounded p-1 mr-3 flex items-center justify-center h-8 w-8">
              <i className="fa-solid fa-building" />
            </div>
            <span className="text-white font-semibold text-lg tracking-wide">MMG Agent</span>
          </div>
          <div className="p-4 space-y-4">
            <button
              type="button"
              onClick={newTask}
              className="w-full bg-teal-500 hover:bg-teal-400 text-white font-medium py-2 px-4 rounded-md flex items-center shadow-sm transition-colors"
            >
              <i className="fa-solid fa-plus mr-2" /> New task
            </button>
            <nav className="space-y-1 mt-6">
              {(
                [
                  ['tasks', 'fa-regular fa-square-check', 'Tasks'],
                  ['connectors', 'fa-solid fa-link', 'Connectors'],
                  ['performance', 'fa-solid fa-chart-bar', 'Performance'],
                ] as const
              ).map(([p, icon, label]) => (
                <button
                  type="button"
                  key={p}
                  onClick={() => nav(p as Page)}
                  className={`w-full text-left flex items-center px-3 py-2 rounded-md transition-colors ${
                    page === p ? 'text-white bg-gray-800' : 'hover:text-white hover:bg-gray-800'
                  }`}
                >
                  <i className={`${icon} w-6 mr-2`} />
                  {label}
                </button>
              ))}
            </nav>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setSettingsOpen(true)}
          className="p-4 border-t border-gray-800 flex items-center cursor-pointer hover:bg-gray-800 transition-colors w-full text-left"
        >
          <div className="bg-teal-900 text-teal-400 rounded-full h-8 w-8 flex items-center justify-center font-bold text-xs mr-3">GN</div>
          <div className="flex-1">
            <p className="text-white text-sm font-medium">Gabriel Navarro</p>
            <p className="text-xs">Broker</p>
          </div>
          <i className="fa-solid fa-chevron-down text-xs" />
        </button>
      </aside>

      <main className="flex-1 flex flex-col relative h-full overflow-hidden">
        {page === 'tasks' && (
          <div className="flex flex-col h-full">
            <header className="h-16 flex items-center justify-between px-8 border-b border-gray-200 bg-white z-10 flex-shrink-0">
              <div>
                <h1 className="font-semibold text-gray-800 text-lg">Tasks</h1>
                <p className="text-xs text-gray-400">History of all agent sessions and generated outputs.</p>
              </div>
              <button
                type="button"
                onClick={newTask}
                className="bg-gray-900 hover:bg-gray-700 text-white text-sm font-medium py-2 px-4 rounded-md flex items-center"
              >
                <i className="fa-solid fa-plus mr-2" /> New task
              </button>
            </header>
            <div className="px-8 pt-8">
              <div className="max-w-4xl bg-gradient-to-r from-teal-500 to-teal-600 rounded-2xl px-8 py-6 text-white shadow-sm">
                <h2 className="text-2xl font-bold mb-1">Welcome back, Gabe 👋</h2>
                <p className="text-teal-100 text-sm">
                  Ready to find your next tenants? Start a new task to search for leads, enrich contact data, and upload to
                  HubSpot.
                </p>
                <button
                  type="button"
                  onClick={newTask}
                  className="mt-4 bg-white text-teal-600 font-semibold text-sm px-5 py-2 rounded-lg hover:bg-teal-50 transition-colors"
                >
                  Start prospecting →
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-y-auto p-8">
              <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
                <table className="w-full text-left text-sm">
                  <thead className="text-xs text-gray-400 uppercase bg-gray-50 border-b border-gray-100">
                    <tr>
                      <th className="px-6 py-3 font-medium">Task</th>
                      <th className="px-6 py-3 font-medium">Status</th>
                      <th className="px-6 py-3 font-medium">Outputs</th>
                      <th className="px-6 py-3 font-medium">Time</th>
                      <th className="px-6 py-3 font-medium" />
                    </tr>
                  </thead>
                  <tbody>
                    {tasks.length === 0 ? (
                      <tr>
                        <td colSpan={5} className="px-6 py-8 text-center text-gray-400">
                          No tasks yet. Click &quot;New task&quot; to get started.
                        </td>
                      </tr>
                    ) : (
                      tasks.map((t) => (
                        <tr
                          key={t.id}
                          className="border-b border-gray-50 hover:bg-gray-50 cursor-pointer"
                          onClick={() => openTask(t.id)}
                        >
                          <td className="px-6 py-4 font-medium text-gray-900">{t.description}</td>
                          <td className="px-6 py-4">
                            {t.status === 'completed' && (
                              <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">
                                <span className="w-1.5 h-1.5 rounded-full bg-green-500 inline-block" /> Completed
                              </span>
                            )}
                            {t.status === 'in_progress' && (
                              <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
                                <span className="w-1.5 h-1.5 rounded-full bg-blue-500 inline-block" /> In Progress
                              </span>
                            )}
                            {t.status === 'failed' && (
                              <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">
                                <span className="w-1.5 h-1.5 rounded-full bg-red-500 inline-block" /> Failed
                              </span>
                            )}
                          </td>
                          <td className="px-6 py-4">
                            {t.outputs.length ? t.outputs.map((o) => (
                              <span
                                key={o}
                                className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700 mr-1"
                              >
                                {o}
                              </span>
                            )) : '—'}
                          </td>
                          <td className="px-6 py-4 text-gray-400 text-xs">{timeAgo(t.time)}</td>
                          <td className="px-6 py-4">
                            <span className="text-xs text-teal-600">Open</span>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
              <div
                className={`mt-4 flex gap-3 ${
                  hasLeads || hasOutreach ? 'flex' : 'hidden'
                }`}
              >
                <a
                  href="/api/download/leads"
                  className="inline-flex items-center gap-2 bg-white border border-gray-200 text-gray-700 text-sm font-medium py-2 px-4 rounded-lg hover:bg-gray-50 shadow-sm"
                >
                  <i className="fa-solid fa-download text-teal-500" /> Download leads.csv
                </a>
                <a
                  href="/api/download/outreach"
                  className="inline-flex items-center gap-2 bg-white border border-gray-200 text-gray-700 text-sm font-medium py-2 px-4 rounded-lg hover:bg-gray-50 shadow-sm"
                >
                  <i className="fa-solid fa-download text-teal-500" /> Download outreach_drafts.csv
                </a>
              </div>
            </div>
          </div>
        )}

        {page === 'session' && (
          <div className="flex flex-col h-full">
            <header className="h-16 flex items-center justify-between px-8 border-b border-gray-200 bg-white z-10 flex-shrink-0">
              <div className="flex items-center gap-3">
                <button type="button" onClick={() => setPage('tasks')} className="text-gray-400 hover:text-gray-600 transition-colors">
                  <i className="fa-solid fa-arrow-left" />
                </button>
                <h1 className="font-semibold text-gray-800 text-lg">{sessionTitle}</h1>
              </div>
              <div className="flex items-center border border-gray-200 rounded-full px-3 py-1 bg-white shadow-sm">
                <div className="w-2 h-2 rounded-full bg-teal-500 mr-2" />
                <span className="text-sm font-medium text-teal-600">Live</span>
              </div>
            </header>
            {messages.length > 0 || isStreaming ? (
              <div className="flex-1 overflow-y-auto px-8 pt-6 pb-36">
                <div className="max-w-3xl mx-auto space-y-6">
                  {messages.map((m) => (
                    <MessageRow key={m.id} m={m} onSendMessage={(t) => void sendMessage(t)} />
                  ))}
                </div>
              </div>
            ) : (
              <div className="flex-1 flex flex-col items-center justify-center px-8 pb-48">
                <div className="w-12 h-12 rounded-xl bg-gray-900 flex items-center justify-center mb-6 shadow-sm">
                  <i className="fa-solid fa-building text-white text-lg" />
                </div>
                <h1 className="text-4xl font-bold text-gray-900 mb-3 text-center">Find your next tenant.</h1>
                <p className="text-gray-400 text-base mb-10 text-center">Florida CRE tenant intelligence, on demand.</p>
                <div className="flex flex-wrap gap-3 justify-center max-w-xl">
                  {['Show me nail salons in Miami-Dade', 'Show me hair salons in Broward', 'Show me barbers in Miami-Dade', 'Show me barbers in Broward'].map(
                    (q) => (
                      <button
                        key={q}
                        type="button"
                        onClick={() => void sendMessage(q)}
                        className="flex items-center gap-2 px-4 py-2 rounded-full border border-gray-200 bg-white text-sm text-gray-600 hover:border-teal-400 hover:text-teal-600 hover:bg-teal-50 transition-all shadow-sm"
                      >
                        {q}
                      </button>
                    ),
                  )}
                </div>
              </div>
            )}

            <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-white via-white to-transparent pt-10 pb-6 px-8 z-20">
              <div className="max-w-3xl mx-auto">
                <div className="relative bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden focus-within:ring-2 focus-within:ring-teal-400 focus-within:border-transparent">
                  <input
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && !isStreaming && void sendMessage()}
                    disabled={isStreaming}
                    type="text"
                    placeholder="Ask anything — find leads, enrich contacts, push to HubSpot…"
                    className="w-full pl-6 pr-14 py-4 bg-transparent focus:outline-none text-gray-700 disabled:opacity-50 text-sm"
                  />
                  {isStreaming ? (
                    <button
                      type="button"
                      onClick={stopStreaming}
                      className="absolute right-3 top-1/2 transform -translate-y-1/2 bg-red-100 hover:bg-red-200 text-red-600 p-2 rounded-xl"
                      title="Stop"
                    >
                      <i className="fa-solid fa-stop text-sm" />
                    </button>
                  ) : (
                    <button
                      type="button"
                      onClick={() => void sendMessage()}
                      className="absolute right-3 top-1/2 transform -translate-y-1/2 bg-gray-900 hover:bg-gray-700 text-white p-2 rounded-xl"
                    >
                      <i className="fa-solid fa-arrow-up text-sm" />
                    </button>
                  )}
                </div>
                <p className="text-center text-xs text-gray-300 mt-3">Local database · Florida business registry · Apollo · HubSpot</p>
              </div>
            </div>
          </div>
        )}

        {page === 'connectors' && (
          <div className="flex flex-col h-full">
            <header className="h-16 flex items-center px-8 border-b border-gray-200 bg-white z-10 flex-shrink-0">
              <div>
                <h1 className="font-semibold text-gray-800 text-lg">Connectors</h1>
                <p className="text-xs text-gray-400">Connect your tools. The agent uses these to take action on your behalf.</p>
              </div>
            </header>
            <div className="flex-1 overflow-y-auto p-8">
              <ConnectorsGrid onOpenSettings={() => setSettingsOpen(true)} />
              <p className="text-xs text-gray-500 mt-6">Open API Configuration from the profile area to add keys.</p>
            </div>
          </div>
        )}

        {page === 'performance' && (
          <div className="flex flex-col h-full">
            <header className="h-16 flex items-center justify-between px-8 border-b border-gray-200 bg-white z-10 flex-shrink-0">
              <div>
                <h1 className="font-semibold text-gray-800 text-lg">Model Performance</h1>
                <p className="text-xs text-gray-400">Compare response time, token usage, leads, and cost across providers.</p>
              </div>
            </header>
            <PerformanceView />
          </div>
        )}

        <SettingsModal
          open={settingsOpen}
          onClose={() => setSettingsOpen(false)}
          onSaveSuccess={(t) => showToast(t)}
        />

        <div
          className={`fixed bottom-6 right-6 z-50 bg-gray-900 text-white text-sm px-5 py-3 rounded-xl shadow-lg transition-all ${
            toastMsg ? '' : 'hidden'
          }`}
        >
          {toastMsg}
        </div>
      </main>
    </div>
  )
}
