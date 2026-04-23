import { useCallback, useId, useState } from 'react'
import type { EmailDraft } from '../types'

type Props = {
  initialDrafts: EmailDraft[]
}

export function EmailDraftsBlock({ initialDrafts }: Props) {
  const baseId = useId()
  const [drafts, setDrafts] = useState<EmailDraft[]>(() =>
    initialDrafts.map((d) => ({ ...d })),
  )
  const [open, setOpen] = useState<Record<number, boolean>>({})

  const markDirty = useCallback(() => {
    // parent could show saved state — kept minimal
  }, [])

  const saveAll = useCallback(async () => {
    try {
      const r = await fetch('/api/save_outreach', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ drafts }),
      })
      const data = await r.json()
      if (data.success) {
        // UI feedback optional
        void data
      }
    } catch (e) {
      console.error('Save failed', e)
    }
  }, [drafts])

  const copyDraft = (d: EmailDraft) => {
    const subject = d.subject_line || ''
    const body = d.email_body || ''
    void navigator.clipboard.writeText(`Subject: ${subject}\n\n${body}`)
  }

  if (!drafts.length) return null

  return (
    <div className="my-4 rounded-2xl border border-gray-200 bg-white shadow-sm overflow-hidden" id={baseId}>
      <div className="flex items-center justify-between px-5 py-3.5 bg-gray-50 border-b border-gray-100">
        <div className="flex items-center gap-2">
          <i className="fa-solid fa-envelopes-bulk text-teal-500" />
          <span className="text-sm font-semibold text-gray-700">
            {drafts.length} Email Draft{drafts.length !== 1 ? 's' : ''}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={saveAll}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-white border border-gray-200 text-gray-600 rounded-lg hover:bg-gray-50 transition-colors shadow-sm"
          >
            <i className="fa-solid fa-floppy-disk text-teal-500" /> Save All
          </button>
          <a
            href="/api/download/outreach"
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-teal-500 text-white rounded-lg hover:bg-teal-600 transition-colors"
          >
            <i className="fa-solid fa-download" /> Export CSV
          </a>
        </div>
      </div>
      <div className="divide-y divide-gray-100 p-3 space-y-2">
        {drafts.map((d, i) => {
          const cardId = `${baseId}-card-${i}`
          const isOpen = open[i]
          return (
            <div key={i} className="border border-gray-200 rounded-xl overflow-hidden bg-white shadow-sm" id={cardId}>
              <div
                className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-gray-50 transition-colors"
                onClick={() => setOpen((o) => ({ ...o, [i]: !o[i] }))}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault()
                    setOpen((o) => ({ ...o, [i]: !o[i] }))
                  }
                }}
                role="button"
                tabIndex={0}
              >
                <div className="flex items-center gap-3 min-w-0">
                  <div className="w-8 h-8 rounded-full bg-teal-50 border border-teal-100 flex items-center justify-center flex-shrink-0">
                    <i className="fa-solid fa-envelope text-teal-500 text-xs" />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-gray-800 truncate">{d.name || '—'}</div>
                    <div className="text-xs text-gray-400 truncate">
                      {d.email || 'no email'} · {d.subject_line || 'no subject'}
                    </div>
                  </div>
                </div>
                <i
                  className={`fa-solid fa-chevron-down text-gray-300 text-xs flex-shrink-0 transition-transform ${
                    isOpen ? 'rotate-180' : ''
                  }`}
                />
              </div>
              {isOpen && (
                <div className="border-t border-gray-100">
                  <div className="px-4 py-3 space-y-3">
                    <div className="flex items-center gap-2 border-b border-gray-100 pb-2">
                      <span className="text-xs font-semibold text-gray-400 w-14 flex-shrink-0">To</span>
                      <input
                        type="email"
                        value={d.email || ''}
                        onChange={(e) => {
                          setDrafts((prev) => {
                            const n = [...prev]
                            n[i] = { ...n[i], email: e.target.value }
                            return n
                          })
                          markDirty()
                        }}
                        className="flex-1 text-sm text-gray-700 outline-none focus:ring-0 bg-transparent placeholder-gray-300"
                        placeholder="recipient@email.com"
                      />
                    </div>
                    <div className="flex items-center gap-2 border-b border-gray-100 pb-2">
                      <span className="text-xs font-semibold text-gray-400 w-14 flex-shrink-0">Subject</span>
                      <input
                        type="text"
                        value={d.subject_line || ''}
                        onChange={(e) => {
                          setDrafts((prev) => {
                            const n = [...prev]
                            n[i] = { ...n[i], subject_line: e.target.value }
                            return n
                          })
                          markDirty()
                        }}
                        className="flex-1 text-sm text-gray-700 outline-none focus:ring-0 bg-transparent placeholder-gray-300"
                        placeholder="Email subject"
                      />
                    </div>
                    <textarea
                      rows={8}
                      value={d.email_body || ''}
                      onChange={(e) => {
                        setDrafts((prev) => {
                          const n = [...prev]
                          n[i] = { ...n[i], email_body: e.target.value }
                          return n
                        })
                        markDirty()
                      }}
                      className="w-full text-sm text-gray-700 outline-none focus:ring-0 bg-transparent resize-none leading-relaxed"
                      placeholder="Email body..."
                    />
                  </div>
                  <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-t border-gray-100">
                    <button
                      type="button"
                      onClick={() => copyDraft(drafts[i]!)}
                      className="inline-flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-700 transition-colors"
                    >
                      <i className="fa-regular fa-copy" /> Copy
                    </button>
                    <button
                      type="button"
                      onClick={saveAll}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-teal-500 text-white rounded-lg hover:bg-teal-600 transition-colors"
                    >
                      <i className="fa-solid fa-floppy-disk" /> Save
                    </button>
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
