/** Feature support per dialect — derived from dbhydra/src implementation. */

export const STATUS = {
  full: { label: 'Supported', className: 'status--full' },
  partial: { label: 'Partial', className: 'status--partial' },
  wip: { label: 'In progress', className: 'status--wip' },
  none: { label: 'Not supported', className: 'status--none' },
}

export const FEATURES = [
  { id: 'connection', label: 'Connection', note: 'Local & remote config via .ini' },
  { id: 'create', label: 'Create table', note: 'DDL create with auto-increment PK' },
  { id: 'drop', label: 'Drop table', note: '' },
  { id: 'select', label: 'Select / query', note: 'SQL or document queries' },
  { id: 'insert', label: 'Insert', note: 'Row or batch insert' },
  { id: 'update', label: 'Update', note: '' },
  { id: 'delete', label: 'Delete', note: '' },
  { id: 'select_to_df', label: 'select_to_df', note: 'Read into pandas DataFrame' },
  { id: 'insert_from_df', label: 'insert_from_df', note: 'Write from pandas DataFrame' },
  { id: 'generate_table_dict', label: 'generate_table_dict', note: 'Introspect live schema' },
  { id: 'schema_ddl', label: 'Schema DDL', note: 'add / drop / modify column' },
  { id: 'migrations', label: 'Migrations', note: 'Migrator & @save_migration' },
  { id: 'transactions', label: 'Transactions', note: 'connect_to_db + transaction()' },
  { id: 'export_xlsx', label: 'export_to_xlsx', note: 'Export table to Excel' },
]

export const DIALECTS = [
  {
    id: 'mysql',
    name: 'MySQL / MariaDB',
    className: 'MysqlDb / MysqlTable',
    maturity: 'Most complete',
    summary:
      'Primary backend with full CRUD, schema DDL, pandas integration, and migration hooks. Best tested dialect.',
  },
  {
    id: 'postgres',
    name: 'PostgreSQL',
    className: 'PostgresDb / PostgresTable',
    maturity: 'Strong',
    summary:
      'CRUD and schema DDL implemented. Type conversion from MySQL-style types. Marked in-progress in docs.',
  },
  {
    id: 'sqlserver',
    name: 'SQL Server',
    className: 'SqlServerDb / SqlServerTable',
    maturity: 'Core CRUD',
    summary:
      'Create, insert, and select work. No schema DDL or migrator integration. macOS connection blocked.',
  },
  {
    id: 'mongo',
    name: 'MongoDB',
    className: 'MongoDb / MongoTable',
    maturity: 'Document API',
    summary:
      'Separate document-oriented API (not AbstractTable). Pandas helpers available; no SQL-style schema DDL.',
  },
  {
    id: 'bigquery',
    name: 'BigQuery',
    className: 'BigQueryDb / BigQueryTable',
    maturity: 'Read-focused',
    summary:
      'Remote-only via service account. Schema introspection and queries; no table writes through Table class.',
  },
  {
    id: 'xlsx',
    name: 'Xlsx / CSV',
    className: 'XlsxDb / XlsxTable',
    maturity: 'File store',
    summary:
      'File-backed tables using pandas. Best for local prototypes; use update_from_df instead of SQL update.',
  },
]

/** full | partial | wip | none */
export const MATRIX = {
  mysql: {
    connection: 'full',
    create: 'full',
    drop: 'full',
    select: 'full',
    insert: 'full',
    update: 'partial',
    delete: 'full',
    select_to_df: 'full',
    insert_from_df: 'full',
    generate_table_dict: 'full',
    schema_ddl: 'full',
    migrations: 'partial',
    transactions: 'full',
    export_xlsx: 'full',
  },
  postgres: {
    connection: 'partial',
    create: 'full',
    drop: 'full',
    select: 'partial',
    insert: 'full',
    update: 'partial',
    delete: 'full',
    select_to_df: 'full',
    insert_from_df: 'full',
    generate_table_dict: 'full',
    schema_ddl: 'partial',
    migrations: 'partial',
    transactions: 'partial',
    export_xlsx: 'full',
  },
  sqlserver: {
    connection: 'partial',
    create: 'full',
    drop: 'full',
    select: 'partial',
    insert: 'full',
    update: 'partial',
    delete: 'full',
    select_to_df: 'full',
    insert_from_df: 'full',
    generate_table_dict: 'partial',
    schema_ddl: 'none',
    migrations: 'none',
    transactions: 'full',
    export_xlsx: 'full',
  },
  mongo: {
    connection: 'full',
    create: 'none',
    drop: 'full',
    select: 'full',
    insert: 'full',
    update: 'full',
    delete: 'full',
    select_to_df: 'full',
    insert_from_df: 'full',
    generate_table_dict: 'partial',
    schema_ddl: 'none',
    migrations: 'none',
    transactions: 'none',
    export_xlsx: 'none',
  },
  bigquery: {
    connection: 'partial',
    create: 'none',
    drop: 'none',
    select: 'partial',
    insert: 'none',
    update: 'none',
    delete: 'none',
    select_to_df: 'partial',
    insert_from_df: 'none',
    generate_table_dict: 'full',
    schema_ddl: 'none',
    migrations: 'none',
    transactions: 'none',
    export_xlsx: 'none',
  },
  xlsx: {
    connection: 'full',
    create: 'full',
    drop: 'full',
    select: 'none',
    insert: 'none',
    update: 'partial',
    delete: 'full',
    select_to_df: 'full',
    insert_from_df: 'full',
    generate_table_dict: 'full',
    schema_ddl: 'none',
    migrations: 'none',
    transactions: 'none',
    export_xlsx: 'full',
  },
}

export const DOCS_SECTIONS = [
  {
    id: 'overview',
    title: 'Overview',
    content: [
      {
        type: 'p',
        text: 'dbhydra is a data-science friendly ORM library combining Python, Pandas, and various SQL and NoSQL dialects. It was built for data scientists, data engineers, and backend developers who want a standardized, intuitive API without heavy model definitions.',
      },
      {
        type: 'list',
        title: 'Design goals',
        items: [
          'Simple, readable database operations',
          'Standardized approach across DB dialects',
          'Object-relational mapping with minimal boilerplate',
          'No detailed models required for existing databases',
          'Fast schema introspection and migrations',
          'First-class pandas DataFrame integration',
        ],
      },
    ],
  },
  {
    id: 'installation',
    title: 'Installation',
    content: [
      {
        type: 'code',
        label: 'Terminal',
        text: 'pip install dbhydra',
      },
      {
        type: 'p',
        text: 'Requires Python 3.6+. Core dependencies include pandas, pymysql, pyodbc, pymongo, google-cloud-bigquery, and pydantic.',
      },
    ],
  },
  {
    id: 'getting-started',
    title: 'Getting started',
    content: [
      {
        type: 'code',
        label: 'Connect and query',
        text: `import dbhydra.dbhydra_core as dh

db1 = dh.MysqlDb("config-mysql.ini")

with db1.connect_to_db():
    table = dh.MysqlTable(db1, "users", ["id", "name"], ["int", "nvarchar"])
    table.create()
    table.insert([[None, "Alice"]])
    rows = table.select_all()
    df = table.select_to_df()`,
      },
      {
        type: 'p',
        text: 'Use a .ini config file with DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD, and LOCALLY flags. Swap MysqlDb for PostgresDb, SqlServerDb, MongoDb, BigQueryDb, or XlsxDb to target a different backend.',
      },
    ],
  },
  {
    id: 'tables',
    title: 'Tables',
    content: [
      {
        type: 'list',
        title: 'Core table operations',
        items: [
          'create() / drop() — manage table lifecycle',
          'select() / select_all() — run queries',
          'insert() / insert_from_df() — add rows',
          'update() / update_from_df() — modify rows',
          'delete() — remove rows',
          'select_to_df() — load into pandas',
          'export_to_xlsx() — export to Excel',
        ],
      },
      {
        type: 'list',
        title: 'Schema introspection',
        items: [
          'db.generate_table_dict() — map of all tables with columns & types',
          'Table.init_all_columns(db, name) — load schema from live DB',
          'Table.init_from_column_type_dict(db, name, column_type_dict) — define schema in Python',
        ],
      },
      {
        type: 'p',
        text: 'MySQL and PostgreSQL also support add_column(), drop_column(), and modify_column() for schema migrations.',
      },
    ],
  },
  {
    id: 'migrations',
    title: 'Migrations',
    content: [
      {
        type: 'p',
        text: 'The migrator tracks schema changes and can apply forward/backward migrations. It is designed for MySQL and PostgreSQL dialects.',
      },
      {
        type: 'code',
        label: 'Migrator example',
        text: `db1.initialize_migrator()

migration = db1.migrator.create_table_migration(
    "users",
    old_column_type_dict,
    new_column_type_dict,
)
db1.migrator.save_current_migration_to_json()`,
      },
      {
        type: 'p',
        text: 'DDL methods on MySQL/Postgres tables are decorated with @save_migration to record changes automatically.',
      },
    ],
  },
  {
    id: 'dialects',
    title: 'Dialect support',
    content: [{ type: 'matrix' }],
  },
]
