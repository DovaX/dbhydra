import { useState } from 'react'
import { DOCS_SECTIONS } from '../data/dialectSupport.js'
import DialectMatrix from './DialectMatrix.jsx'
import InstallCommand from './InstallCommand.jsx'

function DocBlock({ block }) {
  if (block.type === 'p') {
    return <p className="docs__text">{block.text}</p>
  }

  if (block.type === 'list') {
    return (
      <div className="docs__block">
        {block.title ? <h4>{block.title}</h4> : null}
        <ul className="docs__list">
          {block.items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </div>
    )
  }

  if (block.type === 'code') {
    return (
      <div className="docs__block">
        {block.label ? <p className="docs__code-label">{block.label}</p> : null}
        <div className="code-block">
          <pre>
            <code>{block.text}</code>
          </pre>
        </div>
      </div>
    )
  }

  if (block.type === 'matrix') {
    return <DialectMatrix />
  }

  return null
}

export default function DocsSection() {
  const [activeId, setActiveId] = useState(DOCS_SECTIONS[0].id)
  const activeSection = DOCS_SECTIONS.find((section) => section.id === activeId)

  return (
    <section className="section docs" id="docs">
      <div className="section__header">
        <p className="eyebrow">Documentation</p>
        <h2>Everything you need to get started</h2>
        <p className="section__lead">
          Embedded docs for dbhydra — installation, usage, migrations, and a
          code-accurate feature matrix for every supported dialect.
        </p>
      </div>

      <div className="docs__layout">
        <nav className="docs__nav" aria-label="Documentation sections">
          {DOCS_SECTIONS.map((section) => (
            <button
              key={section.id}
              type="button"
              className={`docs__nav-item${activeId === section.id ? ' docs__nav-item--active' : ''}`}
              onClick={() => setActiveId(section.id)}
            >
              {section.title}
            </button>
          ))}
        </nav>

        <article className="docs__panel">
          <h3>{activeSection.title}</h3>

          {activeSection.id === 'installation' ? (
            <InstallCommand variant="featured" />
          ) : null}

          {activeSection.content.map((block, index) => (
            <DocBlock key={`${activeSection.id}-${index}`} block={block} />
          ))}
        </article>
      </div>
    </section>
  )
}
