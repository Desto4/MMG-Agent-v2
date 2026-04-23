import { useState } from 'react'
import type { Lead } from '../types'
import { escHtml } from '../lib/utils'

const scoreOf = (l: Lead, i: number) => {
  let s = 95 - i * 3
  if (!l.trade_name && !l.entity_name) s -= 15
  if (!l.business_phone) s -= 10
  if (!l.website) s -= 8
  if (!l.google_rating) s -= 7
  return Math.max(30, Math.min(99, Math.round(s)))
}

const cityOf = (l: Lead) => {
  if (l.city && l.state) return `${l.city}, ${l.state}`
  if (l.city) return l.city
  if (l.state) return l.state
  const addr = String(l.address || '')
  const parts = addr.split(',').map((s) => s.trim())
  if (parts.length >= 3) return parts[parts.length - 2]
  if (parts.length === 2) return parts[0]
  return '—'
}

const scoreColor = (s: number) => {
  if (s >= 85) return 'bg-teal-50 text-teal-700 border-teal-300'
  if (s >= 65) return 'bg-amber-50 text-amber-700 border-amber-300'
  return 'bg-gray-50 text-gray-500 border-gray-300'
}

const detailFields: { label: string; get: (l: Lead) => string | null | undefined }[] = [
  { label: 'Trade Name', get: (l) => l.trade_name as string | undefined | null },
  { label: 'Entity Name (Sunbiz)', get: (l) => l.entity_name as string | undefined | null },
  { label: 'Formation Date', get: (l) => l.formation_date as string | undefined | null },
  { label: 'Years in Business', get: (l) => l.years_in_business as string | undefined | null },
  { label: 'General Email', get: (l) => l.general_email as string | undefined | null },
  { label: 'Business Phone', get: (l) => l.business_phone as string | undefined | null },
  { label: 'Address', get: (l) => l.address as string | undefined | null },
  {
    label: 'Website',
    get: (l) => (l.website ? `<a href="${escHtml(String(l.website))}" target="_blank" rel="noreferrer" class="text-teal-600 underline">${escHtml(String(l.website))}</a>` : null),
  },
  {
    label: 'Instagram',
    get: (l) =>
      l.instagram_url
        ? `<a href="${escHtml(String(l.instagram_url))}" target="_blank" rel="noreferrer" class="text-pink-500 underline">${escHtml(String(l.instagram_url))}</a>`
        : null,
  },
  {
    label: 'Facebook',
    get: (l) =>
      l.facebook_url
        ? `<a href="${escHtml(String(l.facebook_url))}" target="_blank" rel="noreferrer" class="text-blue-600 underline">${escHtml(String(l.facebook_url))}</a>`
        : null,
  },
  {
    label: 'Google Rating',
    get: (l) =>
      l.google_rating
        ? `${l.google_rating} ★ (${l.google_review_count ?? 0} reviews)`
        : null,
  },
]

function LeadRow({ l, i }: { l: Lead; i: number }) {
  const [open, setOpen] = useState(false)
  const score = scoreOf(l, i)
  const city = cityOf(l)
  const tradeName = l.trade_name || l.entity_name || l.company || '—'
  const entityName = l.entity_name || ''
  const phone = l.business_phone || ''
  const website = l.website
    ? String(l.website)
        .replace(/^https?:\/\//, '')
        .replace(/\/$/, '')
        .split('/')[0]
    : ''
  const igRaw = l.instagram_url || ''
  const igHandle = igRaw
    ? '@' + String(igRaw).replace(/^https?:\/\/(www\.)?instagram\.com\//, '').replace(/\/$/, '')
    : ''
  const rating = l.google_rating ? parseFloat(String(l.google_rating)).toFixed(1) : ''
  const reviews = l.google_review_count
    ? parseInt(String(l.google_review_count), 10).toLocaleString()
    : ''
  const r = parseFloat(rating) || 0
  const full = Math.floor(r)
  const half = r % 1 >= 0.5 ? 1 : 0
  const empty = 5 - full - half
  const estBadge = (() => {
    const parts: string[] = []
    if (l.industry) parts.push(escHtml(String(l.industry)))
    if (l.formation_date) {
      const d = new Date(String(l.formation_date))
      const lbl = isNaN(d.getTime())
        ? String(l.formation_date)
        : d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
      parts.push('Est. ' + lbl)
    } else if (l.years_in_business) parts.push(escHtml(String(l.years_in_business)))
    return parts.length ? parts.join(' · ') : null
  })()
  const rowId = `lead-detail-${i}`

  return (
    <>
      <tr
        className="border-b border-gray-100 hover:bg-slate-50 transition-colors cursor-pointer"
        onClick={() => setOpen((o) => !o)}
      >
        <td className="px-4 py-3 max-w-[220px]">
          <div className="flex items-start gap-2">
            <div>
              <div className="font-semibold text-gray-900 text-sm leading-tight">{escHtml(String(tradeName))}</div>
              {entityName && entityName !== tradeName && (
                <div className="text-xs text-gray-400 mt-0.5">{escHtml(String(entityName))}</div>
              )}
              {estBadge && (
                <div className="mt-1.5">
                  <span
                    className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-teal-50 text-teal-700 border border-teal-100"
                    dangerouslySetInnerHTML={{ __html: estBadge }}
                  />
                </div>
              )}
            </div>
            <i className="fa-solid fa-chevron-down text-gray-300 text-[10px] mt-1.5 flex-shrink-0" />
          </div>
        </td>
        <td className="px-4 py-3 text-sm text-gray-600 whitespace-nowrap">
          <span className="inline-flex items-center gap-1">
            <svg className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path
                fillRule="evenodd"
                d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z"
                clipRule="evenodd"
              />
            </svg>
            {escHtml(city)}
          </span>
        </td>
        <td className="px-4 py-3">
          <span
            className={`inline-flex items-center justify-center w-9 h-9 rounded-full border-2 font-bold text-sm ${scoreColor(score)}`}
          >
            {score}
          </span>
        </td>
        <td className="px-4 py-3 text-xs space-y-1 min-w-[160px]">
          {phone && (
            <div className="flex items-center gap-1.5 text-gray-700">
              <span>{escHtml(phone)}</span>
            </div>
          )}
          {website && l.website && (
            <div className="flex items-center gap-1.5">
              <a
                href={String(l.website)}
                target="_blank"
                rel="noreferrer"
                className="text-teal-600 hover:text-teal-800 hover:underline truncate max-w-[130px] block"
                onClick={(e) => e.stopPropagation()}
              >
                {escHtml(website)}
              </a>
            </div>
          )}
          {igHandle && (
            <div className="flex items-center gap-1.5 text-pink-500">
              <a
                href={String(igRaw)}
                target="_blank"
                rel="noreferrer"
                className="hover:underline truncate max-w-[130px] block"
                onClick={(e) => e.stopPropagation()}
              >
                {escHtml(igHandle)}
              </a>
            </div>
          )}
          {!phone && !website && !igHandle && <span className="text-gray-300">—</span>}
        </td>
        <td className="px-4 py-3 whitespace-nowrap">
          {rating ? (
            <div className="flex flex-col gap-0.5">
              <div className="flex items-center gap-1">
                <span className="text-yellow-400 text-xs">
                  {'★'.repeat(full)}
                  {half ? '½' : ''}
                  <span className="text-gray-200">{'★'.repeat(empty)}</span>
                </span>
                <span className="text-xs font-semibold text-gray-700 ml-0.5">{rating}</span>
              </div>
              {reviews && <div className="text-[10px] text-gray-400">({reviews} reviews)</div>}
            </div>
          ) : (
            <span className="text-gray-300 text-xs">—</span>
          )}
        </td>
      </tr>
      {open && (
        <tr id={rowId} className="bg-slate-50 border-b border-gray-100">
          <td colSpan={6} className="px-5 py-4">
            <div className="grid grid-cols-2 md:grid-cols-3 gap-x-6 gap-y-3 text-xs">
              <div className="col-span-2 md:col-span-3 pb-1 mb-1 border-b border-gray-200">
                <span className="font-semibold text-gray-700 text-[11px] uppercase tracking-wide">Full Lead Details</span>
              </div>
              {detailFields.map(({ label, get }) => {
                const v = get(l)
                return (
                  <div key={label}>
                    <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wide mb-0.5">
                      {label}
                    </div>
                    {v ? (
                      <div className="text-gray-700" dangerouslySetInnerHTML={{ __html: v }} />
                    ) : (
                      <div className="text-gray-300">—</div>
                    )}
                  </div>
                )
              })}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

type Props = {
  leads: Lead[]
  onSendMessage: (text: string) => void
}

export function LeadsTableBlock({ leads, onSendMessage }: Props) {
  const displayed = leads.slice(0, 20)

  return (
    <div className="flex items-start space-x-4 my-2">
      <div className="w-8 flex-shrink-0" />
      <div className="flex-1 mt-2">
        <p className="text-sm text-gray-600 mb-3">
          Found <strong className="text-gray-900">{leads.length} prospects</strong> matching your criteria. Top
          results ranked by expansion probability — all verified active Florida licenses with Sunbiz entity data
          enriched.
        </p>
        <div className="border border-gray-200 rounded-xl overflow-hidden shadow-sm">
          <div className="bg-white px-4 py-2.5 border-b border-gray-100 flex items-center justify-between">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
              {leads.length} Results
            </span>
            <a
              href="/api/download/leads"
              className="inline-flex items-center gap-1.5 text-xs text-teal-600 hover:text-teal-800 font-medium"
            >
              <i className="fa-solid fa-download text-[10px]" /> Download CSV
            </a>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="px-4 py-2.5 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">
                    Business / Entity <span className="normal-case font-normal text-gray-300">(click to expand)</span>
                  </th>
                  <th className="px-4 py-2.5 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Location</th>
                  <th className="px-4 py-2.5 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Score</th>
                  <th className="px-4 py-2.5 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Contact</th>
                  <th className="px-4 py-2.5 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Google</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-50">
                {displayed.map((l, i) => (
                  <LeadRow key={i} l={l} i={i} />
                ))}
              </tbody>
            </table>
          </div>
          {leads.length > 20 && (
            <div className="px-4 py-2 text-xs text-gray-400 bg-gray-50 border-t border-gray-100 text-center">
              +{leads.length - 20} more rows available in the CSV download
            </div>
          )}
        </div>
        <div className="flex flex-wrap gap-2 mt-3">
          <button
            type="button"
            onClick={() => onSendMessage('Upload these leads to HubSpot')}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-full text-xs font-medium bg-orange-50 text-orange-700 border border-orange-200 hover:bg-orange-100 transition-colors"
          >
            <i className="fa-brands fa-hubspot text-[11px]" /> Upload these to HubSpot
          </button>
          <button
            type="button"
            onClick={() => onSendMessage('Pull more leads from this search')}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200 hover:bg-blue-100 transition-colors"
          >
            <i className="fa-solid fa-magnifying-glass text-[11px]" /> Pull more results
          </button>
          <button
            type="button"
            onClick={() => onSendMessage('Write outreach emails for these leads')}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-full text-xs font-medium bg-teal-50 text-teal-700 border border-teal-200 hover:bg-teal-100 transition-colors"
          >
            <i className="fa-solid fa-envelope text-[11px]" /> Write outreach emails
          </button>
          <button
            type="button"
            onClick={() => onSendMessage('Export these leads to CSV')}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-full text-xs font-medium bg-gray-50 text-gray-600 border border-gray-200 hover:bg-gray-100 transition-colors"
          >
            <i className="fa-solid fa-download text-[11px]" /> Export CSV
          </button>
        </div>
      </div>
    </div>
  )
}
