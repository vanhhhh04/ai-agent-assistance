export function AppHeader({
  title,
  subtitle,
  actions,
}: {
  title: string;
  subtitle?: string;
  actions?: React.ReactNode;
}) {
  return (
    <header className="px-6 md:px-8 py-5 border-b border-[color:var(--color-border)] bg-white md:pl-8 pl-16">
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-lg font-bold text-[color:var(--color-text)]">{title}</h1>
          {subtitle && (
            <p className="text-sm text-[color:var(--color-text-subtle)] mt-0.5">{subtitle}</p>
          )}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
    </header>
  );
}
