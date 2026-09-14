import { DIALECTS, FEATURES, MATRIX, STATUS } from '../data/dialectSupport.js'

function StatusCell({ value }) {
  const meta = STATUS[value] ?? STATUS.none
  return (
    <td className={`matrix__cell ${meta.className}`}>
      <span className="matrix__badge">{meta.label}</span>
    </td>
  )
}

export default function DialectMatrix() {
  return (
    <div className="matrix-wrap">
      <div className="matrix-legend">
        {Object.values(STATUS).map((item) => (
          <span key={item.label} className={`matrix-legend__item ${item.className}`}>
            {item.label}
          </span>
        ))}
      </div>

      <div className="matrix-scroll">
        <table className="matrix">
          <thead>
            <tr>
              <th className="matrix__feature-col">Feature</th>
              {DIALECTS.map((dialect) => (
                <th key={dialect.id}>
                  <span className="matrix__dialect-name">{dialect.name}</span>
                  <span className="matrix__dialect-class">{dialect.className}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {FEATURES.map((feature) => (
              <tr key={feature.id}>
                <th scope="row" className="matrix__feature-col">
                  <span>{feature.label}</span>
                  {feature.note ? (
                    <span className="matrix__feature-note">{feature.note}</span>
                  ) : null}
                </th>
                {DIALECTS.map((dialect) => (
                  <StatusCell
                    key={`${dialect.id}-${feature.id}`}
                    value={MATRIX[dialect.id]?.[feature.id] ?? 'none'}
                  />
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="dialect-cards">
        {DIALECTS.map((dialect) => (
          <article className="dialect-card" key={dialect.id}>
            <div className="dialect-card__head">
              <h4>{dialect.name}</h4>
              <span className="dialect-card__maturity">{dialect.maturity}</span>
            </div>
            <p>{dialect.summary}</p>
            <code>{dialect.className}</code>
          </article>
        ))}
      </div>
    </div>
  )
}
