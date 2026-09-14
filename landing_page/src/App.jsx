import './App.css'
import DocsSection from './components/DocsSection.jsx'
import InstallCommand from './components/InstallCommand.jsx'

const FEATURES = [
  {
    title: 'Pandas-native workflows',
    description:
      'Insert from DataFrames, select to DataFrames, and export tables to Excel — built for data scientists, not just backend engineers.',
  },
  {
    title: 'Multi-database support',
    description:
      'One consistent API across MySQL, SQL Server, PostgreSQL, MongoDB, BigQuery, and even Xlsx-backed stores.',
  },
  {
    title: 'Schema introspection',
    description:
      'Generate a table dictionary from live databases, inspect column types, and bootstrap new tables from column_type_dict.',
  },
  {
    title: 'Migrations built in',
    description:
      'Track schema changes with the migrator — create, alter, and drop tables with versioned migration files.',
  },
  {
    title: 'Simple CRUD API',
    description:
      'Create, drop, select, insert, update, and delete with readable Python methods instead of hand-written SQL boilerplate.',
  },
  {
    title: 'Lightweight & MIT licensed',
    description:
      'Install with pip, connect with a config file, and start querying. Open source and free for commercial use.',
  },
]

const CODE_EXAMPLE = `import dbhydra.dbhydra_core as dh

db1 = dh.MysqlDb("config-mysql.ini")

with db1.connect_to_db():
    table_dict = db1.generate_table_dict()
    source_table = table_dict["test"]

    new_table = dh.MysqlTable.init_from_column_type_dict(
        db1,
        "test_new",
        {"id": "int", "name": "nvarchar"},
    )
    new_table.create()

    df = source_table.select_to_df()
    new_table.insert_from_df(df)`

function App() {
  return (
    <div className="page">
      <div className="bg-glow bg-glow--left" aria-hidden="true" />
      <div className="bg-glow bg-glow--right" aria-hidden="true" />

      <header className="header">
        <a className="logo" href="#top">
          <img
            className="logo__icon"
            src="/dbhydra-logo.png"
            alt=""
            width={32}
            height={32}
          />
          dbhydra
        </a>
        <nav className="nav">
          <a href="#features">Features</a>
          <a href="#docs">Docs</a>
          <a href="#example">Example</a>
          <a href="#install">Install</a>
        </nav>
        <a
          className="header__cta"
          href="https://github.com/DovaX/dbhydra"
          target="_blank"
          rel="noreferrer"
        >
          GitHub
        </a>
      </header>

      <main id="top">
        <section className="hero">
          <div className="hero__content">
            <p className="eyebrow">Python ORM for data science</p>
            <h1>
              Connect Python &amp; Pandas to any database with one familiar API
            </h1>
            <p className="hero__lead">
              dbhydra is a lightweight ORM that bridges SQL dialects, NoSQL
              stores, and spreadsheet workflows — so you spend less time on
              plumbing and more time on analysis.
            </p>

            <InstallCommand variant="hero" />

            <div className="hero__actions">
              <a
                className="btn btn--primary"
                href="https://pypi.org/project/dbhydra/"
                target="_blank"
                rel="noreferrer"
              >
                Install from PyPI
              </a>
              <a
                className="btn btn--ghost"
                href="https://github.com/DovaX/dbhydra"
                target="_blank"
                rel="noreferrer"
              >
                View on GitHub
              </a>
            </div>
            <div className="hero__stats">
              <div>
                <strong>One command</strong>
                <span>pip install dbhydra</span>
              </div>
              <div>
                <strong>6+ backends</strong>
                <span>SQL &amp; NoSQL</span>
              </div>
              <div>
                <strong>MIT</strong>
                <span>Open source</span>
              </div>
            </div>
          </div>

          <div className="hero__visual">
            <div className="hero__logo-wrap">
              <img
                className="hero__logo"
                src="/dbhydra-logo.png"
                alt="dbhydra logo — hydra heads connecting to a database"
              />
            </div>
            <div className="hero__panel">
              <div className="terminal">
                <div className="terminal__bar">
                  <span />
                  <span />
                  <span />
                  <p>quickstart.py</p>
                </div>
                <pre>
                  <code>{CODE_EXAMPLE}</code>
                </pre>
              </div>
            </div>
          </div>
        </section>

        <section className="section" id="features">
          <div className="section__header">
            <p className="eyebrow">Why dbhydra</p>
            <h2>Everything you need to move data, not reinvent it</h2>
          </div>
          <div className="feature-grid">
            {FEATURES.map((feature) => (
              <article className="feature-card" key={feature.title}>
                <h3>{feature.title}</h3>
                <p>{feature.description}</p>
              </article>
            ))}
          </div>
        </section>

        <DocsSection />

        <section className="section" id="example">
          <div className="section__header">
            <p className="eyebrow">Usage</p>
            <h2>From connection to DataFrame in a few lines</h2>
          </div>
          <div className="code-block">
            <pre>
              <code>{CODE_EXAMPLE}</code>
            </pre>
          </div>
        </section>

        <section className="section install" id="install">
          <div className="install__card">
            <p className="eyebrow">Installation</p>
            <h2>Start in under a minute</h2>
            <InstallCommand variant="featured" />
            <div className="install__steps">
              <div className="install__step">
                <span>1</span>
                <div>
                  <p>Install the package</p>
                  <code>pip install dbhydra</code>
                </div>
              </div>
              <div className="install__step">
                <span>2</span>
                <div>
                  <p>Configure your database</p>
                  <code>config-mysql.ini</code>
                </div>
              </div>
              <div className="install__step">
                <span>3</span>
                <div>
                  <p>Connect and query</p>
                  <code>with db.connect_to_db(): ...</code>
                </div>
              </div>
            </div>
            <a
              className="btn btn--primary"
              href="https://pypi.org/project/dbhydra/"
              target="_blank"
              rel="noreferrer"
            >
              View on PyPI
            </a>
          </div>
        </section>
      </main>

      <footer className="footer">
        <p>
          <strong>dbhydra</strong> by DovaX · MIT License
        </p>
        <div className="footer__links">
          <a href="https://github.com/DovaX/dbhydra" target="_blank" rel="noreferrer">
            GitHub
          </a>
          <a href="https://pypi.org/project/dbhydra/" target="_blank" rel="noreferrer">
            PyPI
          </a>
        </div>
      </footer>
    </div>
  )
}

export default App
