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
    <label className="field">
      <span>{label}</span>
      <input
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
