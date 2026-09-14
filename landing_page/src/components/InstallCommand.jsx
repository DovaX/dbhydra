import { useState } from 'react'

const INSTALL_CMD = 'pip install dbhydra'

export default function InstallCommand({ variant = 'default' }) {
  const [copied, setCopied] = useState(false)

  async function copyCommand() {
    try {
      await navigator.clipboard.writeText(INSTALL_CMD)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      setCopied(false)
    }
  }

  return (
    <div className={`install-command install-command--${variant}`}>
      <div className="install-command__label">Quick setup</div>
      <div className="install-command__row">
        <span className="install-command__prompt">$</span>
        <code>{INSTALL_CMD}</code>
        <button
          type="button"
          className="install-command__copy"
          onClick={copyCommand}
          aria-label="Copy install command"
        >
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>
    </div>
  )
}
