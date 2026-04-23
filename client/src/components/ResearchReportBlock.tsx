import { escHtml } from '../lib/utils'

type Props = { title: string }

export function ResearchReportBlock({ title }: Props) {
  return (
    <div className="flex items-start space-x-4 my-3">
      <div className="w-8 flex-shrink-0" />
      <div className="flex-1">
        <div className="border border-teal-200 bg-teal-50/40 rounded-xl p-4 flex items-center justify-between gap-4">
          <div className="min-w-0">
            <p className="text-xs font-semibold text-teal-700 uppercase tracking-wide mb-1">Research Report</p>
            <p className="text-sm font-medium text-gray-900 truncate" dangerouslySetInnerHTML={{ __html: escHtml(title) }} />
          </div>
          <a
            href="/api/download/report"
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-medium bg-teal-500 hover:bg-teal-400 text-white flex-shrink-0"
          >
            <i className="fa-solid fa-file-pdf text-[11px]" /> Download PDF
          </a>
        </div>
      </div>
    </div>
  )
}
