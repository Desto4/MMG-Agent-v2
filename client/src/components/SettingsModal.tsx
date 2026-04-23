import { useState, useEffect } from 'react'
import type { ConfigResponse } from '../types'

type Props = {
  open: boolean
  onClose: () => void
  onSaveSuccess: (toast: string) => void
}

export function SettingsModal({ open, onClose, onSaveSuccess }: Props) {
  const [currentProvider, setCurrentProvider] = useState('anthropic')
  const [msg, setMsg] = useState('')

  const [anthropic, setAnthropic] = useState('')
  const [claudeModel, setClaudeModel] = useState('claude-opus-4-6')
  const [gemini, setGemini] = useState('')
  const [geminiModel, setGeminiModel] = useState('gemini-3-flash-preview')
  const [perplexity, setPerplexity] = useState('')
  const [perplexityModel, setPerplexityModel] = useState('sonar-pro')
  const [apollo, setApollo] = useState('')
  const [hunter, setHunter] = useState('')
  const [hubspot, setHubspot] = useState('')
  const [crmPath, setCrmPath] = useState('')
  const [gmailAddress, setGmailAddress] = useState('')
  const [gmailAppPassword, setGmailAppPassword] = useState('')

  useEffect(() => {
    if (!open) return
    setMsg('')
    ;(async () => {
      try {
        const r = await fetch('/api/config')
        const c: ConfigResponse = await r.json()
        if (c.model_provider) setCurrentProvider(c.model_provider)
        if (c.gemini_model) setGeminiModel(c.gemini_model)
      } catch {
        // ignore
      }
    })()
  }, [open])

  if (!open) return null

  const save = async () => {
    const body: Record<string, string> = {
      anthropic_key: anthropic.trim(),
      claude_model: claudeModel,
      apollo_key: apollo.trim(),
      hunter_key: hunter.trim(),
      hubspot_token: hubspot.trim(),
      gemini_key: gemini.trim(),
      gemini_model: geminiModel,
      perplexity_key: perplexity.trim(),
      perplexity_model: perplexityModel,
      model_provider: currentProvider,
      gmail_address: gmailAddress.trim(),
      gmail_app_password: gmailAppPassword.trim(),
      crm_path: crmPath.trim(),
    }
    if (
      !body.anthropic_key &&
      !body.apollo_key &&
      !body.hubspot_token &&
      !body.gemini_key &&
      !body.perplexity_key &&
      !body.crm_path
    ) {
      setMsg('Please enter at least one key.')
      return
    }
    try {
      const resp = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const out = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        onSaveSuccess(`Could not save settings (${resp.status}). ${(out as { error?: string }).error || ''}`.trim())
        return
      }
      setGmailAddress('')
      setGmailAppPassword('')
      onClose()
      const providerLabel =
        currentProvider === 'gemini'
          ? `Gemini (${body.gemini_model})`
          : currentProvider === 'perplexity'
            ? `Perplexity (${body.perplexity_model})`
            : `Claude (${body.claude_model})`
      onSaveSuccess(`Saved — using ${providerLabel}`)
    } catch (e) {
      onSaveSuccess(`Error saving config: ${(e as Error).message}`)
    }
  }

  return (
    <div
      className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="bg-white rounded-2xl shadow-xl w-full max-w-md max-h-[min(90vh,720px)] flex flex-col min-h-0 overflow-hidden">
        <div className="flex items-center justify-between gap-3 px-6 pt-6 pb-3 flex-shrink-0 border-b border-gray-100">
          <h2 className="font-semibold text-gray-900 text-lg">API Configuration</h2>
          <div className="flex items-center gap-2 flex-shrink-0">
            <button
              type="button"
              onClick={save}
              className="bg-teal-500 hover:bg-teal-400 text-white text-sm font-medium px-4 py-2 rounded-lg shadow-sm"
            >
              Save
            </button>
            <button type="button" onClick={onClose} className="text-gray-400 hover:text-gray-600 text-xl leading-none px-1" aria-label="Close">
              &times;
            </button>
          </div>
        </div>
        <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain px-6 py-4 space-y-4">
          <div className="bg-gray-50 rounded-xl p-4 space-y-3">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">AI Model</p>
            <div className="flex gap-2 flex-wrap">
              {(['anthropic', 'gemini', 'perplexity'] as const).map((p) => (
                <button
                  key={p}
                  type="button"
                  onClick={() => setCurrentProvider(p)}
                  className={`flex-1 py-2 px-3 rounded-lg text-sm font-medium border transition-colors ${
                    currentProvider === p
                      ? 'bg-teal-500 text-white border-teal-500'
                      : 'bg-white text-gray-600 border-gray-200'
                  }`}
                >
                  {p === 'anthropic' ? '🤖 Claude' : p === 'gemini' ? '✨ Gemini' : '🔍 Perplexity'}
                </button>
              ))}
            </div>
            {currentProvider === 'anthropic' && (
              <div className="space-y-2">
                <input
                  value={anthropic}
                  onChange={(e) => setAnthropic(e.target.value)}
                  type="password"
                  placeholder="Anthropic API key  (sk-ant-...)"
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500"
                />
                <select
                  value={claudeModel}
                  onChange={(e) => setClaudeModel(e.target.value)}
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500 bg-white"
                >
                  <option value="claude-opus-4-6">claude-opus-4-6 (most capable)</option>
                  <option value="claude-sonnet-4-6">claude-sonnet-4-6</option>
                  <option value="claude-haiku-4-5">claude-haiku-4-5 (fastest)</option>
                </select>
                <p className="text-xs text-gray-400">
                  Get a key at{' '}
                  <a href="https://console.anthropic.com/settings/keys" target="_blank" rel="noreferrer" className="text-teal-500 underline">
                    console.anthropic.com
                  </a>
                </p>
              </div>
            )}
            {currentProvider === 'gemini' && (
              <div className="space-y-2">
                <input
                  value={gemini}
                  onChange={(e) => setGemini(e.target.value)}
                  type="password"
                  placeholder="Gemini API key  (AIza...)"
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
                />
                <select
                  value={geminiModel}
                  onChange={(e) => setGeminiModel(e.target.value)}
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm bg-white"
                >
                  <option value="gemini-3-flash-preview">gemini-3-flash-preview (latest)</option>
                  <option value="gemini-2.0-flash">gemini-2.0-flash</option>
                </select>
              </div>
            )}
            {currentProvider === 'perplexity' && (
              <div className="space-y-2">
                <input
                  value={perplexity}
                  onChange={(e) => setPerplexity(e.target.value)}
                  type="password"
                  placeholder="Perplexity API key  (pplx-...)"
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
                />
                <select
                  value={perplexityModel}
                  onChange={(e) => setPerplexityModel(e.target.value)}
                  className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm bg-white"
                >
                  <option value="sonar-pro">sonar-pro (best)</option>
                  <option value="sonar">sonar</option>
                </select>
              </div>
            )}
          </div>
          <div className="bg-gray-50 rounded-xl p-4 space-y-3">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Data Connectors</p>
            <p className="text-xs text-gray-400">
              Keys are saved in <code className="text-[11px] bg-gray-100 px-1 rounded">.local_api_keys.json</code> (not
              committed to git). Leave a field blank to keep the previous value.
            </p>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Apollo API Key</label>
              <input
                value={apollo}
                onChange={(e) => setApollo(e.target.value)}
                type="password"
                className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Hunter.io Key</label>
              <input
                value={hunter}
                onChange={(e) => setHunter(e.target.value)}
                type="password"
                className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">HubSpot Token</label>
              <input
                value={hubspot}
                onChange={(e) => setHubspot(e.target.value)}
                type="password"
                className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
              />
            </div>
          </div>
          <div className="bg-gray-50 rounded-xl p-4 space-y-3">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Business Leads Database</p>
            <input
              value={crmPath}
              onChange={(e) => setCrmPath(e.target.value)}
              type="text"
              placeholder="/path/to/MMG_Tenant_CRM.xlsx"
              className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm font-mono"
            />
          </div>
          <div className="bg-gray-50 rounded-xl p-4 space-y-3">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Gmail</p>
            <input
              value={gmailAddress}
              onChange={(e) => setGmailAddress(e.target.value)}
              type="email"
              placeholder="you@gmail.com"
              className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
            />
            <input
              value={gmailAppPassword}
              onChange={(e) => setGmailAppPassword(e.target.value)}
              type="password"
              placeholder="App password"
              className="w-full border border-gray-200 rounded-lg px-4 py-2 text-sm"
            />
          </div>
        </div>
        {msg && <div className="px-6 pb-1 text-sm text-red-500 font-medium flex-shrink-0">{msg}</div>}
        <div className="flex gap-3 px-6 py-3 flex-shrink-0 border-t border-gray-100 bg-gray-50/80">
          <button
            type="button"
            onClick={save}
            className="flex-1 bg-teal-500 hover:bg-teal-400 text-white font-medium py-2.5 rounded-lg text-sm shadow-sm"
          >
            Save settings
          </button>
          <button type="button" onClick={onClose} className="flex-1 bg-gray-100 hover:bg-gray-200 text-gray-600 font-medium py-2.5 rounded-lg text-sm">
            Cancel
          </button>
        </div>
      </div>
    </div>
  )
}
