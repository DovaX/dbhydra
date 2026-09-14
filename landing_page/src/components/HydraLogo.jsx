export default function HydraLogo({ className = '', size = 48 }) {
  return (
    <svg
      className={className}
      width={size}
      height={size}
      viewBox="0 0 120 120"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <defs>
        <linearGradient id="hydraGrad" x1="20" y1="20" x2="100" y2="100">
          <stop offset="0%" stopColor="#5eead4" />
          <stop offset="100%" stopColor="#2bb3a5" />
        </linearGradient>
        <linearGradient id="dbGrad" x1="40" y1="58" x2="80" y2="98">
          <stop offset="0%" stopColor="#7dd3fc" />
          <stop offset="100%" stopColor="#3b82a0" />
        </linearGradient>
      </defs>

      <ellipse cx="60" cy="78" rx="28" ry="8" fill="url(#dbGrad)" opacity="0.35" />
      <ellipse cx="60" cy="72" rx="26" ry="7" fill="url(#dbGrad)" />
      <rect x="34" y="72" width="52" height="18" fill="url(#dbGrad)" />
      <ellipse cx="60" cy="72" rx="26" ry="7" fill="#5eb8d9" opacity="0.55" />
      <ellipse cx="60" cy="90" rx="28" ry="8" fill="url(#dbGrad)" />

      <path
        d="M38 58 C42 46, 34 34, 28 24"
        stroke="url(#hydraGrad)"
        strokeWidth="5"
        strokeLinecap="round"
        fill="none"
      />
      <path
        d="M60 58 C60 44, 60 32, 60 20"
        stroke="url(#hydraGrad)"
        strokeWidth="5"
        strokeLinecap="round"
        fill="none"
      />
      <path
        d="M82 58 C78 46, 86 34, 92 24"
        stroke="url(#hydraGrad)"
        strokeWidth="5"
        strokeLinecap="round"
        fill="none"
      />

      <circle cx="28" cy="22" r="7" fill="#3dd6c6" />
      <circle cx="24" cy="21" r="1.4" fill="#042018" />
      <circle cx="60" cy="18" r="7" fill="#3dd6c6" />
      <circle cx="56" cy="17" r="1.4" fill="#042018" />
      <circle cx="92" cy="22" r="7" fill="#3dd6c6" />
      <circle cx="88" cy="21" r="1.4" fill="#042018" />

      <path
        d="M32 58 C36 62, 44 64, 60 64 C76 64, 84 62, 88 58"
        stroke="#3dd6c6"
        strokeWidth="2.5"
        strokeLinecap="round"
        opacity="0.7"
      />
    </svg>
  )
}
