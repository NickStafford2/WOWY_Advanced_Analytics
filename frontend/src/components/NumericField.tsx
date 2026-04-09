import { readNumberValue } from '../app/leaderboardQuery'

type NumericFieldProps = {
  label: string
  value: number
  min?: string
  max?: string
  step?: string
  disabled?: boolean
  onChange: (value: number) => void
}

export function NumericField({
  label,
  value,
  min,
  max,
  step,
  disabled = false,
  onChange,
}: NumericFieldProps) {
  return (
    <label className="flex flex-col gap-1.5 text-[0.9rem] text-[color:var(--text-secondary)]">
      <span>{label}</span>
      <input
        className="min-h-[42px] w-full rounded-xl border border-[color:var(--control-border)] bg-[var(--input-background)] px-[14px] text-[color:var(--text-primary)] disabled:cursor-not-allowed disabled:opacity-60"
        type="number"
        min={min}
        max={max}
        step={step}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(readNumberValue(event.target.value))}
      />
    </label>
  )
}
