import type { ReactNode } from 'react'

import { cn } from '@/lib/utils'

type SectionPanelProps = {
  title: string
  description?: ReactNode
  actions?: ReactNode
  icon?: ReactNode
  children: ReactNode
  className?: string
}

/** A titled dashboard section. Tests find sections by their <h2> title. */
export function SectionPanel({ title, description, actions, icon, children, className }: SectionPanelProps) {
  return (
    <section
      className={cn(
        'rounded-2xl border bg-card/80 p-4 shadow-[0_1px_0_0_rgba(255,255,255,0.03)_inset,0_24px_48px_-32px_rgba(0,0,0,0.45)] backdrop-blur-sm sm:p-6',
        className,
      )}
    >
      <header className="mb-5 flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        <div className="flex min-w-0 items-start gap-3">
          {icon && (
            <span className="mt-0.5 flex size-9 shrink-0 items-center justify-center rounded-lg bg-primary/12 text-primary [&_svg]:size-[18px]">
              {icon}
            </span>
          )}
          <div className="min-w-0">
            <h2 className="text-lg font-semibold tracking-tight text-foreground sm:text-xl">{title}</h2>
            {description && <p className="mt-1 text-sm text-muted-foreground">{description}</p>}
          </div>
        </div>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </header>
      {children}
    </section>
  )
}
