/* Str:::lab Studio — Systems Manager v1.3.0
 * ═══════════════════════════════════════════════════════════════════════
 * Connector JARs & External System Integrations
 *
 * v1.3.0 changes (merged fix release):
 *  - RESTORED: "Restart SQL Gateway session" and "Reconnect Studio session"
 *    buttons from v1.1.0 — these are critical for activating newly uploaded
 *    connector JARs without SSH/terminal access.
 *  - RESTORED: Old connector card grid layout (icon-card grid, category
 *    sections, click-to-expand detail panel) from v1.1.0 — cleaner UX.
 *  - FIX: JAR availability detection now cross-checks nginx /udf-jars/ AND
 *    Flink /jars endpoint live on every modal open — localStorage cache alone
 *    was causing false "Available" badges for JARs not actually in /opt/flink/lib/.
 *  - FIX: Added prominent Kafka topic pre-flight hint: warns user to create
 *    the topic before submitting pipeline (root cause of the TimeoutException).
 *  - FIX: Upload tab now shows copy-ready docker cp command per JAR after upload.
 *  - Added _catProbeViaFlink alias (used by integrations tab probe helpers).
 *
 * Root cause of reported error:
 *   org.apache.kafka.common.errors.TimeoutException:
 *   Topic raw-clicks not present in metadata after 60000 ms.
 *   → The Kafka topic must exist before the Flink job starts.
 *     Either set 'properties.allow.auto.create.topics'='true' in the sink
 *     WITH clause, or create the topic manually:
 *       docker exec <kafka> kafka-topics.sh --create \
 *         --bootstrap-server kafka-01:9092 \
 *         --topic raw-clicks --partitions 3 --replication-factor 1
 *
 * Uses: api(), state, toast(), openModal(), closeModal(), addLog(), escHtml()
 * ═══════════════════════════════════════════════════════════════════════
 */

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR DEFINITIONS (v1.1.0 structure with v1.2.0 extra connectors merged)
// ─────────────────────────────────────────────────────────────────────────────
const CONNECTOR_DEFS = [
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8',
        category: 'messaging',
        jarPattern: 'flink-sql-connector-kafka',
        jarNames: ['flink-sql-connector-kafka', 'flink-connector-kafka'],
        mavenArtifact: 'flink-sql-connector-kafka',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Match to your Flink version: e.g. 3.3.0-1.19 or 3.4.0-2.0',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/',
        desc: 'Apache Kafka source and sink. Required for all Kafka-backed streaming tables — real-time event ingestion, CDC, and microservice pipelines.',
        warning: '⚠ Kafka topics must exist before the Flink job starts. Either pre-create the topic or add \'properties.allow.auto.create.topics\'=\'true\' to your sink WITH clause.',
        sqlExample: `-- ⚠  Create the Kafka topic FIRST (if auto-create is off):
--   docker exec <kafka> kafka-topics.sh --create \\
--     --bootstrap-server kafka-01:9092 \\
--     --topic raw-clicks --partitions 3 --replication-factor 1
--
-- Then define source/sink tables:
CREATE TABLE kafka_source (
  id      BIGINT,
  payload STRING,
  ts      TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw-clicks',
  'properties.bootstrap.servers' = 'kafka-01:9092',
  'properties.group.id'          = 'flink-group-1',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json'
);

CREATE TABLE kafka_sink WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw-clicks',
  'properties.bootstrap.servers' = 'kafka-01:9092',
  'properties.allow.auto.create.topics' = 'true',
  'format'                       = 'json'
) LIKE kafka_source (EXCLUDING ALL);`,
        noJarNeeded: false,
    },
    {
        id: 'jdbc',
        label: 'JDBC (Postgres / MySQL)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/><line x1="6" y1="16" x2="26" y2="16" stroke="#56c4c4" stroke-width="1" stroke-dasharray="3 2"/></svg>`,
        color: '#56c4c4',
        category: 'database',
        jarPattern: 'flink-connector-jdbc',
        jarNames: ['flink-connector-jdbc', 'flink-connector-jdbc-core', 'postgresql', 'mysql-connector'],
        mavenArtifact: 'flink-connector-jdbc',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Also requires the DB driver JAR (postgresql-42.x.x.jar or mysql-connector-j-8.x.x.jar)',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/',
        desc: 'JDBC connector for PostgreSQL, MySQL, MariaDB. Enables reading/writing relational tables from Flink SQL.',
        sqlExample: `CREATE TABLE pg_orders (
  order_id BIGINT,
  status   STRING,
  amount   DOUBLE,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:postgresql://postgres:5432/mydb',
  'table-name'= 'orders',
  'username'  = 'flink_user',
  'password'  = 'secret'
);`,
        noJarNeeded: false,
    },
    {
        id: 'filesystem_s3',
        label: 'Filesystem / S3 / MinIO',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/><line x1="16" y1="8" x2="16" y2="22" stroke="#f5a623" stroke-width="1.5" stroke-dasharray="3 2"/></svg>`,
        color: '#f5a623',
        category: 'storage',
        jarPattern: 'flink-s3-fs-hadoop',
        jarNames: ['flink-s3-fs-hadoop', 'flink-s3-fs-presto'],
        mavenArtifact: 'flink-s3-fs-hadoop',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Built into Flink plugins/ directory. Copy from /opt/flink/plugins/s3-fs-hadoop/ to /opt/flink/lib/',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/',
        desc: 'Filesystem connector for S3, MinIO, GCS, HDFS, and local paths. Supports Parquet, ORC, JSON, CSV.',
        sqlExample: `CREATE TABLE s3_sink (
  event_date STRING,
  event_type STRING,
  amount     DOUBLE
) PARTITIONED BY (event_date)
WITH (
  'connector' = 'filesystem',
  'path'      = 's3://my-bucket/events/',
  'format'    = 'parquet'
);`,
        noJarNeeded: false,
    },
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464',
        category: 'search',
        jarPattern: 'flink-sql-connector-elasticsearch',
        jarNames: ['flink-sql-connector-elasticsearch', 'flink-connector-elasticsearch'],
        mavenArtifact: 'flink-sql-connector-elasticsearch7',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Use elasticsearch7 for ES 7.x and OpenSearch. Use elasticsearch8 for ES 8.x.',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/elasticsearch/',
        desc: 'Elasticsearch 7/8 and OpenSearch sink. Stream aggregated or enriched records directly into search indices.',
        sqlExample: `CREATE TABLE es_metrics (
  symbol  STRING,
  price   DOUBLE,
  ts      TIMESTAMP(3),
  PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts'     = 'http://elasticsearch:9200',
  'index'     = 'market-metrics'
);`,
        noJarNeeded: false,
    },
    {
        id: 'iceberg',
        label: 'Apache Iceberg',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><polygon points="16,4 28,26 4,26" stroke="#4bcffa" stroke-width="1.5" fill="none"/><polygon points="16,11 23,24 9,24" fill="rgba(75,207,250,0.15)" stroke="#4bcffa" stroke-width="1"/></svg>`,
        color: '#4bcffa',
        category: 'lakehouse',
        jarPattern: 'iceberg-flink-runtime',
        jarNames: ['iceberg-flink-runtime', 'iceberg-flink'],
        mavenArtifact: 'iceberg-flink-runtime-1.19',
        mavenGroup: 'org.apache.iceberg',
        versionNote: 'Format: iceberg-flink-runtime-<FLINK_MAJOR>-<ICEBERG_VER>.jar',
        downloadUrl: 'https://iceberg.apache.org/releases/',
        docUrl: 'https://iceberg.apache.org/docs/latest/flink/',
        desc: 'Apache Iceberg table format — ACID transactions, schema evolution, time travel on S3/HDFS.',
        sqlExample: `CREATE TABLE iceberg_orders (
  order_id BIGINT,
  status   STRING,
  amount   DOUBLE,
  dt       STRING
) PARTITIONED BY (dt)
WITH (
  'connector'    = 'iceberg',
  'catalog-name' = 'my_iceberg_catalog',
  'catalog-type' = 'hive',
  'warehouse'    = 's3://bucket/warehouse/'
);`,
        noJarNeeded: false,
    },
    {
        id: 'hive',
        label: 'Apache Hive',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731',
        category: 'lakehouse',
        jarPattern: 'flink-connector-hive',
        jarNames: ['flink-connector-hive', 'flink-sql-connector-hive'],
        mavenArtifact: 'flink-connector-hive_2.12',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Must match both Flink and Hive versions.',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        desc: 'Apache Hive connector for reading/writing Hive tables and using the Hive Metastore as a catalog.',
        sqlExample: `CREATE CATALOG hive_catalog WITH (
  'type'                = 'hive',
  'hive.metastore.uris' = 'thrift://hive-metastore:9083'
);
USE CATALOG hive_catalog;
USE my_hive_database;
SELECT * FROM hive_table LIMIT 10;`,
        noJarNeeded: false,
    },
    {
        id: 'mongodb',
        label: 'MongoDB',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M16 4 C16 4 10 12 10 18 A6 6 0 0 0 22 18 C22 12 16 4 16 4Z" stroke="#57a23e" stroke-width="1.5" fill="none"/><line x1="16" y1="24" x2="16" y2="30" stroke="#57a23e" stroke-width="1.5"/></svg>`,
        color: '#57a23e',
        category: 'database',
        jarPattern: 'flink-connector-mongodb',
        jarNames: ['flink-connector-mongodb'],
        mavenArtifact: 'flink-connector-mongodb',
        mavenGroup: 'org.apache.flink',
        versionNote: '1.2.0-1.19 — match to your Flink version',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/mongodb/',
        desc: 'MongoDB source and sink. Ideal for document-oriented storage, API enrichment, and flexible schema pipelines.',
        sqlExample: `CREATE TABLE mongo_sink (
  _id     STRING,
  user_id BIGINT,
  event   STRING
) WITH (
  'connector'  = 'mongodb',
  'uri'        = 'mongodb://mongo:27017',
  'database'   = 'analytics',
  'collection' = 'events'
);`,
        noJarNeeded: false,
    },
    {
        id: 'datagen',
        label: 'Datagen (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="4" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/><rect x="14" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="4" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="14" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/></svg>`,
        color: '#57c764',
        category: 'testing',
        jarPattern: null,
        jarNames: [],
        mavenArtifact: null,
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/datagen/',
        desc: 'Built-in synthetic data generator. No external dependency needed. Use for dev, load testing, and demos.',
        sqlExample: `CREATE TABLE orders_datagen (
  order_id BIGINT,
  amount   DOUBLE,
  ts       TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '100'
);`,
        noJarNeeded: true,
    },
    {
        id: 'print',
        label: 'Print / Blackhole (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="8" stroke="#6e7274" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="3" fill="#6e7274" opacity="0.5"/></svg>`,
        color: '#8c8fa6',
        category: 'testing',
        jarPattern: null,
        jarNames: [],
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/print/',
        desc: 'Print sink writes rows to TaskManager stdout. Blackhole sink discards all records for benchmarking.',
        sqlExample: `-- Print sink (debug): outputs to TaskManager logs
CREATE TABLE debug_out WITH (
  'connector'        = 'print',
  'print-identifier' = 'DEBUG'
) LIKE source_table (EXCLUDING ALL);

-- Blackhole sink (throughput benchmark)
CREATE TABLE bench_sink WITH ('connector' = 'blackhole')
LIKE source_table (EXCLUDING ALL);`,
        noJarNeeded: true,
    },
];

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION / SYSTEM DEFINITIONS
// ─────────────────────────────────────────────────────────────────────────────
const SYSTEM_DEFS = [
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8',
        category: 'messaging',
        requiresConnectorJar: true,
        connectorJarId: 'kafka',
        authModes: ['none', 'sasl_plain', 'sasl_ssl'],
        testFn: async (fields) => {
            const bs = (fields.bootstrap_servers || '').trim();
            if (!bs) return { ok: false, msg: 'Bootstrap Servers not set.', detail: 'Enter at least one broker address first.' };
            const firstHost = bs.split(',')[0].trim();
            const [host, port] = firstHost.includes(':') ? firstHost.split(':') : [firstHost, '9092'];
            return _catProbeViaFlink(host, port, 'Kafka broker');
        },
        fields: [
            { id: 'bootstrap_servers', label: 'Bootstrap Servers', placeholder: 'kafka-01:9092', required: true, hint: 'Comma-separated broker addresses.' },
            { id: 'topic', label: 'Default Topic', placeholder: 'raw-clicks', required: false, hint: 'Topic must already exist — or enable auto.create.topics.' },
            { id: 'group_id', label: 'Consumer Group ID', placeholder: 'flink-consumer-01', required: false },
            { id: 'format', label: 'Message Format', placeholder: 'json', required: false, isSelect: true,
                options: ['json','avro','avro-confluent','protobuf','csv','raw','debezium-json','canal-json'] },
            { id: 'schema_registry_url', label: 'Schema Registry URL (opt)', placeholder: 'http://schema-registry:8081', required: false },
        ],
        generateSql: (f, auth) => {
            const bs    = f.bootstrap_servers || 'kafka-01:9092';
            const topic = f.topic || 'YOUR_TOPIC';
            const tbl   = f.table_name || 'kafka_stream';
            const fmt   = f.format || 'json';
            const grp   = f.group_id || 'flink-group-1';
            const authProps = [];
            if (auth === 'sasl_plain') {
                authProps.push(`  'properties.security.protocol' = 'SASL_PLAINTEXT'`);
                authProps.push(`  'properties.sasl.mechanism'    = 'PLAIN'`);
                authProps.push(`  'properties.sasl.jaas.config'  = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user || 'USER'}" password="${f.sasl_pass || 'PASS'}";'`);
            } else if (auth === 'sasl_ssl') {
                authProps.push(`  'properties.security.protocol'  = 'SASL_SSL'`);
                authProps.push(`  'properties.sasl.mechanism'     = 'PLAIN'`);
                authProps.push(`  'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user || 'API_KEY'}" password="${f.sasl_pass || 'API_SECRET'}";'`);
                authProps.push(`  'properties.ssl.endpoint.identification.algorithm' = 'https'`);
            }
            const sourcePropList = [
                `  'connector'                    = 'kafka'`,
                `  'topic'                        = '${topic}'`,
                `  'properties.bootstrap.servers' = '${bs}'`,
                `  'properties.group.id'          = '${grp}'`,
                `  'scan.startup.mode'            = 'latest-offset'`,
                `  'format'                       = '${fmt}'`,
                ...authProps,
            ];
            const sinkPropList = [
                `  'connector'                    = 'kafka'`,
                `  'topic'                        = '${topic}'`,
                `  'properties.bootstrap.servers' = '${bs}'`,
                `  'properties.allow.auto.create.topics' = 'true'`,
                `  'format'                       = '${fmt}'`,
                ...authProps,
            ];
            return `-- ⚠  Ensure topic '${topic}' exists first:
--   docker exec <kafka> kafka-topics.sh --create \\
--     --bootstrap-server ${bs} \\
--     --topic ${topic} --partitions 3 --replication-factor 1

CREATE TABLE ${tbl}_source (
  id      BIGINT,
  payload STRING,
  ts      TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
${sourcePropList.join(',\n')}
);

CREATE TABLE ${tbl}_sink (
  id      BIGINT,
  payload STRING,
  ts      TIMESTAMP(3)
) WITH (
${sinkPropList.join(',\n')}
);`;
        },
    },
    {
        id: 'postgres',
        label: 'PostgreSQL',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/></svg>`,
        color: '#56c4c4',
        category: 'database',
        requiresConnectorJar: true,
        connectorJarId: 'jdbc',
        authModes: ['userpass'],
        testFn: async (fields) => {
            const host = (fields.host || '').trim();
            const port = (fields.port || '5432').trim();
            if (!host) return { ok: false, msg: 'Host not set.', detail: 'Enter the PostgreSQL hostname first.' };
            return _catProbeViaFlink(host, port, 'PostgreSQL');
        },
        fields: [
            { id: 'host',     label: 'Host',     placeholder: 'localhost or postgres', required: true },
            { id: 'port',     label: 'Port',     placeholder: '5432',                  required: false },
            { id: 'database', label: 'Database', placeholder: 'mydb',                  required: true },
            { id: 'schema',   label: 'Schema',   placeholder: 'public',                required: false },
        ],
        generateSql: (f, auth) => {
            const url = `jdbc:postgresql://${f.host || 'localhost'}:${f.port || '5432'}/${f.database || 'mydb'}`;
            return `CREATE TABLE pg_table (\n  id     BIGINT,\n  name   STRING,\n  value  DOUBLE,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'       = '${url}',\n  'table-name'= '${f.schema ? f.schema+'.' : ''}YOUR_TABLE',\n  'username'  = '${f.username || 'flink_user'}',\n  'password'  = '${f.password || 'secret'}'\n);`;
        },
    },
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464',
        category: 'search',
        requiresConnectorJar: true,
        connectorJarId: 'elasticsearch',
        authModes: ['none', 'userpass'],
        testFn: async (fields) => {
            const hosts = (fields.hosts || '').trim();
            if (!hosts) return { ok: false, msg: 'Hosts not set.', detail: 'Enter the Elasticsearch host URL first.' };
            return _sysProbeHttp(hosts.split(',')[0].trim(), 'Elasticsearch');
        },
        fields: [
            { id: 'hosts',      label: 'Hosts',         placeholder: 'http://elasticsearch:9200', required: true },
            { id: 'index',      label: 'Default Index', placeholder: 'my-index',                  required: false },
            { id: 'es_version', label: 'Version',       required: false, isSelect: true, options: ['7','8'] },
        ],
        generateSql: (f, auth) => {
            const authPart = (auth === 'userpass' && f.username)
                ? `,\n  'username'  = '${f.username}',\n  'password'  = '${f.password || ''}'` : '';
            return `CREATE TABLE es_sink (\n  id      STRING,\n  score   DOUBLE,\n  ts      TIMESTAMP(3),\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${f.es_version || '7'}',\n  'hosts'     = '${f.hosts || 'http://elasticsearch:9200'}',\n  'index'     = '${f.index || 'my-index'}'${authPart}\n);`;
        },
    },
    {
        id: 'minio',
        label: 'MinIO / S3',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/></svg>`,
        color: '#f5a623',
        category: 'storage',
        requiresConnectorJar: false,
        authModes: ['access_keys'],
        testFn: async (fields) => {
            const ep = (fields.endpoint || '').trim();
            if (!ep) return { ok: false, msg: 'Endpoint URL not set.', detail: 'Enter the MinIO/S3 endpoint URL first.' };
            return _sysProbeHttp(ep, 'MinIO/S3');
        },
        fields: [
            { id: 'endpoint',   label: 'Endpoint URL',          placeholder: 'http://minio:9000',  required: true },
            { id: 'bucket',     label: 'Bucket Name',           placeholder: 'my-data-lake',       required: true },
            { id: 'region',     label: 'Region',                placeholder: 'us-east-1',          required: false },
            { id: 'path_style', label: 'Path Style Access',     required: false, isSelect: true, options: ['true','false'] },
        ],
        generateSql: (f) => {
            return `SET 's3.endpoint'          = '${f.endpoint || 'http://minio:9000'}';\nSET 's3.access-key'        = '${f.aws_access_key || 'YOUR_ACCESS_KEY'}';\nSET 's3.secret-key'        = '${f.aws_secret_key || 'YOUR_SECRET_KEY'}';\nSET 's3.path.style.access' = '${f.path_style || 'true'}';\n\nCREATE TABLE minio_sink (\n  event_date STRING,\n  amount     DOUBLE\n) PARTITIONED BY (event_date)\nWITH (\n  'connector' = 'filesystem',\n  'path'      = 's3://${f.bucket || 'my-bucket'}/events/',\n  'format'    = 'parquet'\n);`;
        },
    },
    {
        id: 'hive_metastore',
        label: 'Hive Metastore',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731',
        category: 'lakehouse',
        requiresConnectorJar: true,
        connectorJarId: 'hive',
        authModes: ['none', 'kerberos'],
        testFn: async (fields) => {
            const uri = (fields.metastore_uri || '').trim();
            if (!uri) return { ok: false, msg: 'Metastore URI not set.', detail: 'Enter the thrift:// URI first.' };
            const m = uri.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '9083') : '9083';
            if (!host) return { ok: false, msg: 'Invalid Metastore URI.', detail: 'Expected: thrift://host:9083' };
            return _catProbeViaFlink(host, port, 'Hive Metastore (thrift)');
        },
        fields: [
            { id: 'metastore_uri',    label: 'Metastore URI(s)',  placeholder: 'thrift://hive-metastore:9083', required: true },
            { id: 'hive_version',     label: 'Hive Version',      placeholder: '3.1.3',                       required: false },
            { id: 'default_database', label: 'Default Database',  placeholder: 'default',                     required: false },
        ],
        generateSql: (f) => {
            return `CREATE CATALOG hive_catalog WITH (\n  'type'                = 'hive',\n  'hive.metastore.uris' = '${f.metastore_uri || 'thrift://hive-metastore:9083'}'${f.hive_version ? `,\n  'hive-version'        = '${f.hive_version}'` : ''}\n);\nUSE CATALOG hive_catalog;\nUSE ${f.default_database || 'default'};\nSHOW TABLES;`;
        },
    },
];

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY BASE RESOLVER
// ─────────────────────────────────────────────────────────────────────────────
function _sysGatewayBase() {
    if (typeof state === 'undefined') return window.location.origin;
    const gw = state?.gateway;
    if (!gw) return window.location.origin;
    if (typeof gw === 'object' && gw.baseUrl) {
        const base = gw.baseUrl.replace(/\/+$/, '').replace('/flink-api', '');
        return base.startsWith('http') ? base : (window.location.origin + base);
    }
    if (typeof gw === 'string') {
        return gw.replace(/\/+$/, '').replace('/flink-api', '').replace('/v1', '');
    }
    return window.location.origin;
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTIVITY PROBE HELPERS
// ─────────────────────────────────────────────────────────────────────────────
async function _sysProbeHttp(url, label) {
    const cleanUrl = url.replace(/\/+$/, '');
    try {
        const r = await fetch(cleanUrl, { signal: AbortSignal.timeout(6000), mode: 'cors' });
        if (r.ok || r.status === 401 || r.status === 403) {
            let detail = cleanUrl + ' → HTTP ' + r.status;
            try {
                const json = await r.json();
                if (json?.version?.number) detail = 'Version: ' + json.version.number + ' — ' + cleanUrl;
                else if (json?.tagline)    detail = json.tagline + ' — ' + cleanUrl;
            } catch(_) {}
            return { ok: true, msg: label + ' reachable ✓', detail };
        }
        return { ok: false, msg: 'HTTP ' + r.status + ' from ' + label, detail: cleanUrl + ' returned an error.' };
    } catch(_) {
        try {
            await fetch(cleanUrl, { signal: AbortSignal.timeout(5000), mode: 'no-cors' });
            return { ok: true, msg: label + ' reachable ✓ (browser CORS policy)', detail: cleanUrl + ' is alive. CORS blocks response body but Flink jobs are unaffected.' };
        } catch(e) {
            return { ok: false, msg: label + ' unreachable from browser', detail: 'Could not reach ' + cleanUrl + '. Check network/VPN/Docker bridge.' };
        }
    }
}

// _catProbeViaFlink is used by SYSTEM_DEFS testFn entries
async function _catProbeViaFlink(host, port, label) {
    const studioBase = _sysGatewayBase();
    const portNum    = parseInt(port) || 0;
    const httpPorts  = new Set([9200, 9000, 9001, 8080, 8081, 8082, 3000]);
    if (httpPorts.has(portNum)) {
        try {
            const r = await fetch(`http://${host}:${port}`, { signal: AbortSignal.timeout(3000), mode: 'no-cors' });
            return { ok: true, msg: label + ' reachable from browser ✓', detail: `${host}:${port} responded.` };
        } catch(_) {}
    }
    try {
        const infoUrl = studioBase + '/flink-api/v1/info';
        const r = await fetch(infoUrl, { signal: AbortSignal.timeout(5000) });
        if (r.ok || r.status < 500) {
            let flinkVer = '';
            try { const j = await r.json(); flinkVer = j?.['flink-version'] ? ' (Flink ' + j['flink-version'] + ')' : ''; } catch(_) {}
            return {
                ok: true,
                msg: 'Studio→Flink reachable' + flinkVer + ' ✓',
                detail: `Studio proxy healthy. To verify ${label} (${host}:${port}) from inside Flink:\n  docker exec <flink-container> bash -c "nc -zv ${host} ${port} && echo OPEN || echo CLOSED"`
            };
        }
        return { ok: false, msg: 'Studio→Flink returned HTTP ' + r.status, detail: 'Check that the Flink SQL Gateway is running.' };
    } catch(e) {
        return {
            ok: false,
            msg: 'Studio proxy unreachable: ' + (e.message || 'timeout'),
            detail: `Manual check:\n  nc -zv ${host} ${port}\n  docker exec <flink-container> bash -c "nc -zv ${host} ${port}"`
        };
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JAR AVAILABILITY — live multi-source detection (nginx + Flink JM + localStorage)
// ─────────────────────────────────────────────────────────────────────────────
function _sysGetUploadedJarNames() {
    try {
        const reg     = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        const udfJars = JSON.parse(localStorage.getItem('strlabstudio_uploaded_jars') || '[]');
        const all     = [...reg.map(e => e.jarName || e), ...udfJars.map(e => e.name || e)];
        return all.filter(Boolean).map(n => n.toLowerCase());
    } catch(_) { return []; }
}

function _sysRecordJarUpload(jarName) {
    try {
        const reg = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        if (!reg.find(e => (e.jarName||e).toLowerCase() === jarName.toLowerCase())) {
            reg.push({ jarName, uploadedAt: Date.now() });
            localStorage.setItem('strlabstudio_connector_jars', JSON.stringify(reg));
        }
    } catch(_) {}
}

async function _sysFetchNginxJarList() {
    try {
        const r = await fetch(window.location.origin + '/udf-jars/', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const text = await r.text();
        try { return JSON.parse(text).map(f => (f.name || f).toLowerCase()).filter(Boolean); } catch(_) { return []; }
    } catch(_) { return []; }
}

async function _sysFetchFlinkJarList() {
    try {
        const base = _sysGatewayBase();
        const jmBase = base.replace('/flink-api', '').replace(':8083', ':8081');
        const r = await fetch(jmBase + '/jars', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const data = await r.json();
        return (data.files || []).map(f => (f.name || '').toLowerCase()).filter(Boolean);
    } catch(_) { return []; }
}

// ─────────────────────────────────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────────────────────────────────
window._sysMgrState = window._sysMgrState || {
    integrationTab: null,
    authMode: 'none',
    savedIntegrations: [],
    liveJarNames: [],
};
(function() {
    try {
        const raw = localStorage.getItem('strlabstudio_integrations');
        if (raw) window._sysMgrState.savedIntegrations = JSON.parse(raw);
    } catch(_) {}
})();

function _sysSaveIntegration(entry) {
    try {
        const list = window._sysMgrState.savedIntegrations;
        const idx  = list.findIndex(e => e.id === entry.id && e.systemId === entry.systemId);
        if (idx >= 0) list[idx] = entry; else list.push(entry);
        localStorage.setItem('strlabstudio_integrations', JSON.stringify(list));
    } catch(_) {}
}

// ─────────────────────────────────────────────────────────────────────────────
// OPEN
// ─────────────────────────────────────────────────────────────────────────────
function openSystemsManager() {
    if (!document.getElementById('modal-systems-manager')) _sysBuildModal();
    openModal('modal-systems-manager');
    _sysSwitchTab('connectors');
    _sysRefreshAvailability();
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD MODAL
// ─────────────────────────────────────────────────────────────────────────────
function _sysBuildModal() {
    const m = document.createElement('div');
    m.id = 'modal-systems-manager';
    m.className = 'modal-overlay';

    const categories = [...new Set(CONNECTOR_DEFS.map(c => c.category))];
    const categoryLabels = { messaging:'Messaging', database:'Database', storage:'Storage', search:'Search', lakehouse:'Lakehouse', testing:'Testing & Dev' };

    m.innerHTML = `
<div class="modal" style="width:920px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">
  <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);">
        <span style="color:var(--blue,#4fa3e0);">⊙</span> Systems Manager
      </div>
      <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Connector JARs &amp; External System Integrations · v1.3.0</div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;">
      <span id="sys-avail-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
      <button onclick="_sysRefreshAvailability()" style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">⟳ Check JARs</button>
      <button class="modal-close" onclick="closeModal('modal-systems-manager')">×</button>
    </div>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
    <button id="sys-tab-connectors"   onclick="_sysSwitchTab('connectors')"   class="udf-tab-btn">📦 Connector JARs</button>
    <button id="sys-tab-upload"       onclick="_sysSwitchTab('upload')"       class="udf-tab-btn">⬆ Upload JAR</button>
    <button id="sys-tab-integrations" onclick="_sysSwitchTab('integrations')" class="udf-tab-btn">⊙ Integrations</button>
    <button id="sys-tab-saved"        onclick="_sysSwitchTab('saved')"        class="udf-tab-btn">💾 Saved</button>
    <button id="sys-tab-guide"        onclick="_sysSwitchTab('guide')"        class="udf-tab-btn">? Guide</button>
  </div>

  <div style="flex:1;overflow-y:auto;min-height:0;">

    <!-- ══ CONNECTORS TAB ════════════════════════════════════════════════ -->
    <div id="sys-pane-connectors" style="padding:16px;display:none;">
      <div style="font-size:11px;color:var(--text2);line-height:1.7;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 13px;margin-bottom:14px;">
        Connectors marked <span style="background:rgba(245,166,35,0.15);color:#f5a623;padding:1px 5px;border-radius:2px;font-size:9px;font-weight:700;">JAR REQ</span> need their JAR in <code>/opt/flink/lib/</code> and a Gateway restart.
        <span style="background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);padding:1px 5px;border-radius:2px;font-size:9px;font-weight:700;">DETECTED</span> = JAR found on disk.
        <strong style="color:var(--yellow);">⚠ Kafka:</strong> Topics must exist before pipeline submit.
      </div>
      ${categories.map(cat => `
        <div style="margin-bottom:16px;">
          <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border);">${categoryLabels[cat] || cat}</div>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:6px;" id="sys-cat-grid-${cat}">
            ${CONNECTOR_DEFS.filter(c => c.category === cat).map(c => _sysConnectorCardHtml(c, [])).join('')}
          </div>
        </div>`).join('')}
      <div id="sys-conn-detail" style="display:none;margin-top:4px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);">
        <div id="sys-conn-detail-body" style="padding:14px;"></div>
      </div>
    </div>

    <!-- ══ UPLOAD JAR TAB ════════════════════════════════════════════════ -->
    <div id="sys-pane-upload" style="padding:20px;display:none;">
      <div style="background:rgba(79,163,224,0.05);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:11px 14px;margin-bottom:14px;font-size:11px;color:var(--text1);line-height:1.8;">
        <strong style="color:var(--blue,#4fa3e0);">Connector JARs must be in /opt/flink/lib/</strong> on all TaskManagers and the SQL Gateway <em>at startup</em>.<br>
        After uploading a JAR, use the <strong>Restart SQL Gateway session</strong> button below — no SSH needed.
      </div>

      <!-- Quick download links -->
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">Quick Download Links</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:5px;">
          ${CONNECTOR_DEFS.filter(c => !c.noJarNeeded && c.downloadUrl).map(c => `
            <a href="${c.downloadUrl}" target="_blank" style="display:flex;align-items:center;gap:8px;padding:7px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);text-decoration:none;">
              <span>${c.icon}</span>
              <div style="min-width:0;flex:1;">
                <div style="font-size:11px;font-weight:600;color:var(--text0);">${c.label}</div>
                <div style="font-size:9px;color:var(--text3);margin-top:1px;">${c.versionNote}</div>
              </div>
              <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="var(--text3)" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>`).join('')}
        </div>
      </div>

      <!-- Drop zone -->
      <div id="sys-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
        onclick="document.getElementById('sys-jar-input').click()"
        ondragover="event.preventDefault();this.style.borderColor='var(--blue,#4fa3e0)';this.style.background='rgba(79,163,224,0.04)'"
        ondragleave="this.style.borderColor='var(--border2)';this.style.background='var(--bg1)'"
        ondrop="_sysJarDrop(event)">
        <div style="font-size:26px;margin-bottom:6px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);">Drop connector JAR here or click to browse</div>
        <div style="font-size:11px;color:var(--text3);margin-top:4px;">Accepts <code>.jar</code> files · Max 256 MB</div>
        <input type="file" id="sys-jar-input" accept=".jar" style="display:none;" onchange="_sysJarFileSelected(event)" />
      </div>
      <div id="sys-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <span>📦</span>
          <div style="flex:1;"><div id="sys-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div><div id="sys-jar-fsize" style="color:var(--text3);font-size:11px;"></div></div>
          <button onclick="_sysJarClear()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
        </div>
      </div>
      <div id="sys-jar-status" style="font-size:12px;min-height:16px;margin-bottom:12px;line-height:1.8;"></div>
      <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;" onclick="_sysJarUpload()">⬆ Upload Connector JAR</button>

      <!-- JAR list -->
      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on Studio Container (/udf-jars/)</span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_sysJarLoadList()">⟳ Refresh</button>
        </div>
        <div id="sys-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div></div>
      </div>

      <!-- ⟳ Restart / Reconnect Controls — restored from v1.1.0 -->
      <div style="margin-top:20px;border-top:1px solid var(--border);padding-top:16px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">After Uploading JARs — Activate Without SSH</div>
        <div style="background:rgba(245,166,35,0.06);border:1px solid rgba(245,166,35,0.2);border-radius:var(--radius);padding:11px 14px;font-size:11px;color:var(--text1);line-height:1.8;margin-bottom:12px;">
          <strong style="color:var(--yellow);">⚠ Connector JARs require a Gateway restart to take effect.</strong><br>
          Use the buttons below to trigger a graceful restart directly from the UI — no SSH or terminal needed.
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;">
          <button class="btn btn-secondary" style="font-size:11px;border-color:rgba(245,166,35,0.4);color:var(--yellow);"
            onclick="_sysRestartGateway()">
            ⟳ Restart SQL Gateway session
          </button>
          <button class="btn btn-secondary" style="font-size:11px;"
            onclick="_sysReconnectSession()">
            ⟲ Reconnect Studio session
          </button>
        </div>
        <div id="sys-restart-status" style="font-size:11px;min-height:14px;font-family:var(--mono);"></div>
        <div style="font-size:11px;color:var(--text3);line-height:1.8;margin-top:8px;">
          <strong>How restart works:</strong><br>
          • <em>Restart SQL Gateway session</em> — closes the current session and opens a new one. Flink reloads JARs from <code>/opt/flink/lib/</code>. Studio auto-reconnects.<br>
          • <em>Reconnect Studio session</em> — reconnects without restarting. Use after an external restart (<code>docker restart flink-sql-gateway</code>).<br>
          • If JARs were uploaded to Studio but not yet copied to Flink: <code>docker cp flink-studio:/var/www/udf-jars/&lt;jar&gt; flink-jobmanager:/opt/flink/lib/</code>
        </div>
      </div>
    </div>

    <!-- ══ INTEGRATIONS TAB ══════════════════════════════════════════════ -->
    <div id="sys-pane-integrations" style="padding:16px;display:none;">
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">Select System</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:6px;">
          ${SYSTEM_DEFS.map(s => `
            <div id="sys-sys-card-${s.id}" onclick="_sysSelectSystem('${s.id}')"
              style="padding:9px 11px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:center;gap:9px;">
              <span>${s.icon}</span>
              <div>
                <div style="font-size:11px;font-weight:700;color:var(--text0);">${s.label.split('(')[0].trim()}</div>
                <div style="font-size:9px;color:var(--text3);margin-top:2px;text-transform:capitalize;">${s.category}</div>
              </div>
            </div>`).join('')}
        </div>
      </div>
      <div id="sys-integration-form" style="display:none;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);">
        <div id="sys-integration-form-body" style="padding:16px;"></div>
      </div>
      <div id="sys-integration-empty" style="text-align:center;padding:28px 0;color:var(--text3);">
        <div style="font-size:28px;margin-bottom:8px;opacity:0.35;">⊙</div>
        <div style="font-size:12px;">Select a system above to configure a connection</div>
      </div>
    </div>

    <!-- ══ SAVED TAB ═════════════════════════════════════════════════════ -->
    <div id="sys-pane-saved" style="padding:16px;display:none;">
      <div id="sys-saved-list"></div>
    </div>

    <!-- ══ GUIDE TAB ═════════════════════════════════════════════════════ -->
    <div id="sys-pane-guide" style="padding:20px;display:none;">
      <div style="display:flex;flex-direction:column;gap:14px;">

        <div style="background:rgba(255,77,109,0.06);border:1px solid rgba(255,77,109,0.25);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--red);margin-bottom:8px;">🔴 Common Error: Topic not in metadata after 60000 ms</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <code>TimeoutException: Topic raw-clicks not present in metadata after 60000 ms</code><br><br>
            This means the Kafka topic does not exist yet. Fix with <strong>either</strong>:<br>
            <strong>Option A — Pre-create the topic</strong> (recommended for production):
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--red);border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:6px 0;">docker exec &lt;kafka-container&gt; kafka-topics.sh --create \\
  --bootstrap-server kafka-01:9092 \\
  --topic raw-clicks \\
  --partitions 3 --replication-factor 1</pre>
            <strong>Option B — Enable auto-create in sink WITH clause</strong> (quick dev fix):
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--yellow);border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:6px 0;">CREATE TABLE raw_clicks_sink WITH (
  'connector'                           = 'kafka',
  'topic'                               = 'raw-clicks',
  'properties.bootstrap.servers'        = 'kafka-01:9092',
  'properties.allow.auto.create.topics' = 'true',
  'format'                              = 'json'
) LIKE clicks_gen (EXCLUDING ALL);</pre>
          </div>
        </div>

        <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--blue,#4fa3e0);margin-bottom:8px;">📦 Connector JAR vs ADD JAR</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <strong>1. Connector JARs</strong> → <code>/opt/flink/lib/</code> — loaded at Flink startup, cluster-wide. Use <em>this Systems Manager</em>.<br>
            <strong>2. UDF JARs</strong> → uploaded per-session via <code>ADD JAR</code>. Use <em>UDF Manager → Upload JAR</em>.<br>
            <span style="color:var(--red);">Never use <code>ADD JAR</code> for connector JARs</span> — they must be in <code>/opt/flink/lib/</code> before Flink starts.
          </div>
        </div>

        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">🐳 Docker: Copy JAR to Flink container</div>
          <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--blue,#4fa3e0);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># 1. Upload the JAR to Studio (Upload JAR tab above)
# 2. Copy from Studio container to Flink containers:
docker cp flink-studio:/var/www/udf-jars/flink-sql-connector-kafka-3.3.0-1.19.jar \\
  flink-jobmanager:/opt/flink/lib/
docker cp flink-studio:/var/www/udf-jars/flink-sql-connector-kafka-3.3.0-1.19.jar \\
  flink-taskmanager:/opt/flink/lib/

# 3. Restart the Flink cluster to pick up new JARs:
docker restart flink-jobmanager flink-taskmanager flink-sql-gateway

# 4. Reconnect in Studio — use "Reconnect Studio session" button in Upload tab
# 5. Verify: SHOW TABLES;  should now list Kafka-backed tables</pre>
        </div>

        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">⊙ Test Connectivity — How It Works</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <strong>HTTP services</strong> (Elasticsearch, MinIO): direct browser fetch — shows version info if available.<br>
            <strong>TCP services</strong> (Kafka, PostgreSQL, Hive Metastore): probes via Studio → Flink REST, gives exact <code>nc -zv</code> command.<br>
            <strong>CORS note</strong>: if a test shows "CORS restricted", the service IS running — browser can't read the response, Flink jobs are unaffected.
          </div>
        </div>
      </div>
    </div>

  </div><!-- /body -->

  <div class="modal-footer" style="display:flex;flex-shrink:0;justify-content:space-between;align-items:center;border-top:1px solid var(--border);background:var(--bg2);padding:12px 20px;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/" target="_blank" style="color:var(--blue,#4fa3e0);text-decoration:none;">📖 Connector Docs ↗</a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/" target="_blank" style="color:var(--blue,#4fa3e0);text-decoration:none;">📖 Kafka Docs ↗</a>
    </div>
    <button class="btn btn-primary" onclick="closeModal('modal-systems-manager')">Close</button>
  </div>
</div>`;

    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('modal-systems-manager'); });

    if (!document.getElementById('sys-mgr-css')) {
        const s = document.createElement('style');
        s.id = 'sys-mgr-css';
        s.textContent = `
.sys-conn-selected { border-color:var(--blue,#4fa3e0)!important;background:rgba(79,163,224,0.06)!important; }
.sys-sys-selected  { border-color:var(--accent)!important;background:rgba(78,157,232,0.07)!important; }
.sys-badge-jar-req  { background:rgba(245,166,35,0.15);color:#f5a623;padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-builtin  { background:rgba(87,198,100,0.12);color:var(--green);padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-detected { display:inline-flex;align-items:center;gap:4px;background:rgba(79,163,224,0.12);border:1px solid rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-detected::before { content:'';width:6px;height:6px;border-radius:50%;background:var(--blue,#4fa3e0);animation:sys-glow 1.8s ease-in-out infinite;flex-shrink:0; }
@keyframes sys-glow { 0%,100%{opacity:1} 50%{opacity:0.5} }
`;
        document.head.appendChild(s);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR CARD HTML HELPER
// ─────────────────────────────────────────────────────────────────────────────
function _sysConnectorCardHtml(c, liveJarNames) {
    const allUploaded = [..._sysGetUploadedJarNames(), ...liveJarNames.map(n => n.toLowerCase())];
    let badgeHtml;
    if (c.noJarNeeded) {
        badgeHtml = `<span class="sys-badge-builtin">✓ BUILT-IN</span>`;
    } else {
        const found = (c.jarNames || []).some(frag => allUploaded.some(name => name.includes(frag.toLowerCase())));
        badgeHtml = found
            ? `<span class="sys-badge-detected">DETECTED</span>`
            : `<span class="sys-badge-jar-req">JAR REQ</span>`;
    }
    return `
<div id="sys-conn-card-${c.id}" onclick="_sysSelectConnector('${c.id}')"
  style="padding:10px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:flex-start;gap:10px;transition:border-color 0.15s;">
  <span style="flex-shrink:0;">${c.icon}</span>
  <div style="flex:1;min-width:0;">
    <div style="font-size:11px;font-weight:700;color:var(--text0);">${c.label}</div>
    <div style="font-size:10px;color:var(--text2);line-height:1.5;margin-top:2px;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;">${c.desc}</div>
    <div style="margin-top:5px;">${badgeHtml}</div>
  </div>
</div>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// AVAILABILITY REFRESH — live multi-source, re-renders cards
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRefreshAvailability() {
    const statusEl = document.getElementById('sys-avail-status');
    if (statusEl) statusEl.textContent = '⟳ checking…';
    try {
        const [nginxJars, flinkJars] = await Promise.all([_sysFetchNginxJarList(), _sysFetchFlinkJarList()]);
        const allLive = [...nginxJars, ...flinkJars];
        window._sysMgrState.liveJarNames = allLive;
        allLive.forEach(name => { if (name.endsWith('.jar')) _sysRecordJarUpload(name); });

        // Re-render each connector card with live data
        CONNECTOR_DEFS.forEach(c => {
            const card = document.getElementById(`sys-conn-card-${c.id}`);
            if (!card) return;
            const newHtml = _sysConnectorCardHtml(c, allLive);
            const tmp = document.createElement('div');
            tmp.innerHTML = newHtml;
            const newCard = tmp.firstElementChild;
            if (newCard) card.replaceWith(newCard);
        });

        const reqConnectors  = CONNECTOR_DEFS.filter(c => !c.noJarNeeded);
        const availableCount = reqConnectors.filter(conn =>
            (conn.jarNames || []).some(frag => allLive.some(name => name.includes(frag.toLowerCase())))
        ).length;
        if (statusEl) statusEl.textContent = `${availableCount}/${reqConnectors.length} JARs detected`;
    } catch(_) {
        if (statusEl) statusEl.textContent = 'offline check';
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TAB SWITCHING
// ─────────────────────────────────────────────────────────────────────────────
function _sysSwitchTab(tab) {
    ['connectors','upload','integrations','saved','guide'].forEach(t => {
        const btn  = document.getElementById(`sys-tab-${t}`);
        const pane = document.getElementById(`sys-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'saved')  _sysRenderSaved();
    if (tab === 'upload') _sysJarLoadList();
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR DETAIL PANEL
// ─────────────────────────────────────────────────────────────────────────────
function _sysSelectConnector(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    CONNECTOR_DEFS.forEach(c => {
        const card = document.getElementById(`sys-conn-card-${c.id}`);
        if (card) card.classList.toggle('sys-conn-selected', c.id === id);
    });
    const detail = document.getElementById('sys-conn-detail');
    const body   = document.getElementById('sys-conn-detail-body');
    if (!detail || !body) return;
    detail.style.display = 'block';

    const allUploaded = [..._sysGetUploadedJarNames(), ...(window._sysMgrState.liveJarNames || []).map(n => n.toLowerCase())];
    const found        = def.noJarNeeded || (def.jarNames || []).some(frag => allUploaded.some(name => name.includes(frag.toLowerCase())));

    const warningHtml = def.warning
        ? `<div style="background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.3);border-radius:4px;padding:8px 12px;font-size:11px;color:var(--yellow);margin-bottom:10px;line-height:1.7;">${def.warning}</div>`
        : '';

    body.innerHTML = `
    <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:12px;">
      <div style="font-size:24px;">${def.icon}</div>
      <div style="flex:1;">
        <div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div>
        <div style="font-size:11px;color:var(--text2);line-height:1.7;margin-top:3px;">${def.desc}</div>
        <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;">
          ${def.noJarNeeded
        ? `<span style="background:rgba(87,198,100,0.12);color:var(--green);padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">✓ No JAR — Built into Flink</span>`
        : found
            ? `<span class="sys-badge-detected" style="padding:2px 8px;font-size:10px;">JAR DETECTED on disk</span>`
            : `<span style="background:rgba(245,166,35,0.12);color:#f5a623;padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">⚠ JAR Required → /opt/flink/lib/</span>`}
          ${def.downloadUrl ? `<a href="${def.downloadUrl}" target="_blank" style="font-size:10px;color:var(--blue,#4fa3e0);padding:2px 8px;border-radius:2px;border:1px solid rgba(79,163,224,0.3);text-decoration:none;">⬇ Download ↗</a>` : ''}
          ${def.docUrl      ? `<a href="${def.docUrl}"      target="_blank" style="font-size:10px;color:var(--text2);padding:2px 8px;border-radius:2px;border:1px solid var(--border);text-decoration:none;">📖 Docs ↗</a>` : ''}
        </div>
        ${!def.noJarNeeded ? `<div style="font-size:10px;color:var(--text3);margin-top:5px;">${def.versionNote}</div>` : ''}
      </div>
    </div>
    ${warningHtml}
    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Example SQL</div>
    <div style="position:relative;">
      <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;max-height:280px;overflow-y:auto;margin:0;">${escHtml(def.sqlExample)}</pre>
      <button onclick="_sysCopyConnectorSql('${id}')" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertConnectorSql('${id}')">↗ Insert into Editor</button>
      <button class="btn btn-primary" style="font-size:11px;" onclick="_sysSwitchTab('integrations');_sysSelectSystem('${id}')">Configure Integration →</button>
    </div>`;
}

function _sysCopyConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    navigator.clipboard.writeText(def.sqlExample).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    const ed  = document.getElementById('sql-editor'); if (!ed) return;
    const s   = ed.selectionStart;
    ed.value  = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + def.sqlExample + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager');
    toast(`${def.label} SQL inserted`, 'ok');
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION FORM
// ─────────────────────────────────────────────────────────────────────────────
function _sysSelectSystem(id) {
    const def = SYSTEM_DEFS.find(s => s.id === id); if (!def) return;
    window._sysMgrState.integrationTab = id;
    window._sysMgrState.authMode = def.authModes?.[0] || 'none';

    SYSTEM_DEFS.forEach(s => {
        const card = document.getElementById(`sys-sys-card-${s.id}`);
        if (card) card.classList.toggle('sys-sys-selected', s.id === id);
    });

    const form  = document.getElementById('sys-integration-form');
    const body  = document.getElementById('sys-integration-form-body');
    const empty = document.getElementById('sys-integration-empty');
    if (!form || !body) return;
    form.style.display  = 'block';
    if (empty) empty.style.display = 'none';

    const saved = (window._sysMgrState.savedIntegrations || []).find(e => e.systemId === id) || {};

    const authTabsHtml = (def.authModes || []).length > 1
        ? `<div style="display:flex;gap:0;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;margin-bottom:10px;">
        ${(def.authModes || []).map((mode, i) => `
          <button id="sys-auth-tab-${mode}" onclick="_sysSelectAuth('${id}','${mode}')"
            style="padding:5px 12px;font-size:11px;font-weight:600;background:${i===0?'var(--accent)':'var(--bg3)'};color:${i===0?'#000':'var(--text2)'};border:none;${i>0?'border-left:1px solid var(--border);':''}cursor:pointer;">
            ${{ none:'No Auth', userpass:'Username / Password', sasl_plain:'SASL Plain', sasl_ssl:'SASL/SSL', bearer:'Bearer Token', access_keys:'AWS Keys', kerberos:'Kerberos' }[mode] || mode}
          </button>`).join('')}
      </div>` : '';

    const fieldsHtml = def.fields.map(f => {
        const savedVal = saved[f.id] || '';
        if (f.isSelect) {
            return `<div style="flex:1;min-width:140px;">
        <label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label>
        <select id="sys-field-${f.id}" class="field-input" style="font-size:12px;" onchange="_sysBuildIntegrationPreview('${id}')">
          ${(f.options||[]).map(o => `<option value="${o}" ${savedVal===o?'selected':''}>${o}</option>`).join('')}
        </select>
        ${f.hint ? `<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>` : ''}
      </div>`;
        }
        return `<div style="flex:1;min-width:140px;">
      <label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label>
      <input id="sys-field-${f.id}" class="field-input" type="text"
        placeholder="${escHtml(f.placeholder||'')}" value="${escHtml(savedVal)}"
        style="font-size:12px;font-family:var(--mono);"
        oninput="_sysBuildIntegrationPreview('${id}')" />
      ${f.hint ? `<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>` : ''}
    </div>`;
    });

    const fieldRows = [];
    for (let i = 0; i < fieldsHtml.length; i += 2) {
        fieldRows.push(`<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">${fieldsHtml[i]}${fieldsHtml[i+1]||''}</div>`);
    }

    body.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;border-bottom:1px solid var(--border);padding-bottom:12px;">
      ${def.icon}
      <div>
        <div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div>
        <div style="font-size:10px;color:var(--text2);margin-top:2px;">${def.category} · ${def.requiresConnectorJar ? 'Connector JAR required' : 'No JAR required'}</div>
      </div>
    </div>

    <div style="margin-bottom:12px;">
      <label class="field-label">Integration Name <span style="color:var(--red);">*</span></label>
      <input id="sys-integration-name" class="field-input" type="text"
        placeholder="${def.label.toLowerCase().replace(/[^a-z0-9]/g,'_')}_prod"
        value="${escHtml(saved.name||'')}"
        style="font-size:12px;font-family:var(--mono);" oninput="_sysBuildIntegrationPreview('${id}')" />
    </div>

    ${authTabsHtml ? `<div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Authentication</div>${authTabsHtml}` : ''}
    <div id="sys-auth-fields-${id}"></div>

    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin:10px 0 6px;">Connection Details</div>
    ${fieldRows.join('')}

    <div style="margin-bottom:12px;">
      <button id="sys-test-btn-${id}" onclick="_sysRunTest('${id}')"
        style="width:100%;padding:7px 12px;font-size:11px;font-weight:600;border-radius:4px;
        background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.4);
        color:var(--blue,#4fa3e0);cursor:pointer;font-family:var(--mono);letter-spacing:.3px;">
        ⊙ Test Connectivity
      </button>
      <div id="sys-test-result-${id}" style="display:none;margin-top:7px;padding:8px 12px;border-radius:4px;font-size:11px;font-family:var(--mono);line-height:1.7;white-space:pre-wrap;"></div>
    </div>

    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Generated Flink SQL</div>
    <div style="position:relative;">
      <pre id="sys-integration-preview"
        style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};
        border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);
        color:var(--text2);white-space:pre-wrap;margin:0 0 12px;line-height:1.7;min-height:48px;">-- Fill in connection details above</pre>
      <button onclick="_sysCopyIntegrationSql()" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
    </div>

    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertIntegrationSql('${id}')">↗ Insert into Editor</button>
      <button class="btn btn-primary" style="font-size:12px;padding:8px 20px;font-weight:700;" onclick="_sysSaveIntegrationForm('${id}')">💾 Save Integration</button>
    </div>`;

    _sysSelectAuth(id, def.authModes?.[0] || 'none', saved);
    _sysBuildIntegrationPreview(id);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTIVITY TEST
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRunTest(systemId) {
    const def    = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const btn    = document.getElementById(`sys-test-btn-${systemId}`);
    const result = document.getElementById(`sys-test-result-${systemId}`);
    if (!result) return;
    if (btn) { btn.disabled = true; btn.textContent = '⊙ Testing…'; }
    result.style.display = 'none';
    const fields = _sysCollectIntegrationFields(systemId);
    let res = { ok: false, msg: 'No test function defined.', detail: '' };
    try {
        if (def.testFn) res = await def.testFn(fields);
    } catch(e) {
        res = { ok: false, msg: 'Test error: ' + (e.message || 'unknown'), detail: '' };
    }
    result.style.display    = 'block';
    result.style.background = res.ok ? 'rgba(63,185,80,0.08)' : 'rgba(224,92,92,0.08)';
    result.style.border     = res.ok ? '1px solid rgba(63,185,80,0.35)' : '1px solid rgba(224,92,92,0.35)';
    result.style.color      = res.ok ? 'var(--green)' : 'var(--red)';
    result.textContent      = (res.ok ? '✓ ' : '✗ ') + res.msg + (res.detail ? '\n' + res.detail : '');
    if (btn) { btn.disabled = false; btn.textContent = '⊙ Test Connectivity'; }
    if (typeof addLog === 'function') addLog(res.ok ? 'OK' : 'WARN', `Systems Manager connectivity test [${systemId}]: ${res.msg}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTH
// ─────────────────────────────────────────────────────────────────────────────
function _sysSelectAuth(systemId, mode, savedVals) {
    window._sysMgrState.authMode = mode;
    const container = document.getElementById(`sys-auth-fields-${systemId}`);
    if (!container) return;
    document.querySelectorAll(`[id^="sys-auth-tab-"]`).forEach(btn => { btn.style.background = 'var(--bg3)'; btn.style.color = 'var(--text2)'; });
    const activeBtn = document.getElementById(`sys-auth-tab-${mode}`);
    if (activeBtn) { activeBtn.style.background = 'var(--accent)'; activeBtn.style.color = '#000'; }
    const saved = savedVals || {};
    const authFields = {
        none: [],
        userpass:    [{ id:'username', label:'Username', placeholder:'flink_user', type:'text' }, { id:'password', label:'Password', placeholder:'••••••••', type:'password' }],
        sasl_plain:  [{ id:'sasl_user', label:'SASL Username / API Key', placeholder:'API_KEY', type:'text' }, { id:'sasl_pass', label:'SASL Password / Secret', placeholder:'API_SECRET', type:'password' }],
        sasl_ssl:    [{ id:'sasl_user', label:'API Key', placeholder:'API_KEY', type:'text' }, { id:'sasl_pass', label:'API Secret', placeholder:'API_SECRET', type:'password' }],
        bearer:      [{ id:'token', label:'Bearer Token', placeholder:'eyJhbGci…', type:'password' }],
        access_keys: [{ id:'aws_access_key', label:'Access Key ID', placeholder:'AKIA…', type:'text' }, { id:'aws_secret_key', label:'Secret Access Key', placeholder:'••••••••', type:'password' }],
        kerberos:    [{ id:'kerberos_principal', label:'Kerberos Principal', placeholder:'flink@REALM.COM', type:'text' }, { id:'kerberos_keytab', label:'Keytab Path', placeholder:'/etc/security/flink.keytab', type:'text' }],
    };
    const fields = authFields[mode] || [];
    if (!fields.length) { container.innerHTML = `<div style="font-size:11px;color:var(--text3);margin-bottom:10px;">No credentials required.</div>`; return; }
    container.innerHTML = `<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
    ${fields.map(f => `
      <div style="flex:1;min-width:140px;">
        <label class="field-label">${f.label}</label>
        <input id="sys-auth-${f.id}" class="field-input" type="${f.type}"
          placeholder="${escHtml(f.placeholder)}" value="${escHtml(saved[f.id]||'')}"
          style="font-size:12px;font-family:var(--mono);"
          oninput="_sysBuildIntegrationPreview('${systemId}')" />
      </div>`).join('')}
  </div>`;
    _sysBuildIntegrationPreview(systemId);
}

function _sysCollectIntegrationFields(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return {};
    const vals = {};
    (def.fields || []).forEach(f => { vals[f.id] = (document.getElementById(`sys-field-${f.id}`)?.value || '').trim(); });
    ['username','password','sasl_user','sasl_pass','token','aws_access_key','aws_secret_key','kerberos_principal','kerberos_keytab'].forEach(k => {
        const el = document.getElementById(`sys-auth-${k}`); if (el) vals[k] = el.value.trim();
    });
    vals.table_name = (document.getElementById('sys-integration-name')?.value || '').trim().replace(/[^a-z0-9_]/gi, '_').toLowerCase() || def.id;
    return vals;
}

function _sysBuildIntegrationPreview(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const prev = document.getElementById('sys-integration-preview'); if (!prev) return;
    const vals = _sysCollectIntegrationFields(systemId);
    const auth = window._sysMgrState.authMode || 'none';
    try { prev.textContent = def.generateSql(vals, auth); } catch(_) { prev.textContent = '-- Fill in connection details above'; }
}

function _sysCopyIntegrationSql() {
    const prev = document.getElementById('sys-integration-preview');
    if (!prev || prev.textContent.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    navigator.clipboard.writeText(prev.textContent).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertIntegrationSql(systemId) {
    const prev = document.getElementById('sys-integration-preview');
    const sql  = prev?.textContent || '';
    if (!sql || sql.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager');
    toast('Integration SQL inserted', 'ok');
}

function _sysSaveIntegrationForm(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const name = (document.getElementById('sys-integration-name')?.value || '').trim();
    if (!name) { toast('Enter an integration name first', 'warn'); return; }
    const vals  = _sysCollectIntegrationFields(systemId);
    const sql   = document.getElementById('sys-integration-preview')?.textContent || '';
    const entry = { id: name, systemId, systemLabel: def.label, savedAt: new Date().toISOString(), authMode: window._sysMgrState.authMode, fields: vals, sql };
    _sysSaveIntegration(entry);
    toast(`Integration "${name}" saved`, 'ok');
    if (typeof addLog === 'function') addLog('OK', `Systems Manager: saved integration "${name}" (${def.label})`);
}

// ─────────────────────────────────────────────────────────────────────────────
// SAVED TAB
// ─────────────────────────────────────────────────────────────────────────────
function _sysRenderSaved() {
    const list  = document.getElementById('sys-saved-list'); if (!list) return;
    const saved = window._sysMgrState.savedIntegrations || [];
    if (!saved.length) {
        list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No saved integrations yet.</div>';
        return;
    }
    list.innerHTML = saved.map((entry, idx) => {
        const sysDef = SYSTEM_DEFS.find(s => s.id === entry.systemId) || {};
        return `
      <div style="border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:8px;overflow:hidden;">
        <div style="padding:9px 12px;background:var(--bg1);display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border);">
          ${sysDef.icon || ''}
          <div style="flex:1;">
            <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:var(--text0);">${escHtml(entry.id)}</span>
            <span style="font-size:10px;color:var(--text3);margin-left:8px;">${escHtml(entry.systemLabel||entry.systemId)}</span>
          </div>
          <span style="font-size:10px;color:var(--text3);">${entry.savedAt ? new Date(entry.savedAt).toLocaleString() : ''}</span>
        </div>
        <pre style="background:var(--bg0);padding:8px 12px;font-size:10px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;margin:0;max-height:80px;overflow-y:auto;">${escHtml((entry.sql||'').slice(0,400))}</pre>
        <div style="padding:8px 12px;display:flex;gap:7px;">
          <button onclick="_sysLoadIntegration(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Load</button>
          <button onclick="_sysSavedInsert(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Insert SQL</button>
          <button onclick="_sysSavedDelete(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;margin-left:auto;">Delete</button>
        </div>
      </div>`;
    }).join('');
}

function _sysLoadIntegration(idx) {
    const entry = (window._sysMgrState.savedIntegrations || [])[idx]; if (!entry) return;
    _sysSwitchTab('integrations');
    _sysSelectSystem(entry.systemId);
    setTimeout(() => {
        Object.entries(entry.fields || {}).forEach(([k, v]) => {
            const el = document.getElementById(`sys-field-${k}`) || document.getElementById(`sys-auth-${k}`);
            if (el) el.value = v;
        });
        const nameEl = document.getElementById('sys-integration-name');
        if (nameEl) nameEl.value = entry.id;
        _sysBuildIntegrationPreview(entry.systemId);
        toast(`Loaded integration "${entry.id}"`, 'ok');
    }, 150);
}

function _sysSavedInsert(idx) {
    const entry = (window._sysMgrState.savedIntegrations || [])[idx]; if (!entry) return;
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + entry.sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager');
    toast('SQL inserted', 'ok');
}

function _sysSavedDelete(idx) {
    if (!confirm('Delete this saved integration?')) return;
    window._sysMgrState.savedIntegrations.splice(idx, 1);
    try { localStorage.setItem('strlabstudio_integrations', JSON.stringify(window._sysMgrState.savedIntegrations)); } catch(_) {}
    _sysRenderSaved();
}

// ─────────────────────────────────────────────────────────────────────────────
// JAR UPLOAD TAB
// ─────────────────────────────────────────────────────────────────────────────
let _sysSelJar = null;

function _sysJarDrop(e) {
    e.preventDefault();
    const dz = document.getElementById('sys-jar-dropzone');
    if (dz) { dz.style.borderColor = 'var(--border2)'; dz.style.background = 'var(--bg1)'; }
    const f = e.dataTransfer?.files?.[0]; if (f) _sysJarSetFile(f);
}
function _sysJarFileSelected(e) { const f = e.target?.files?.[0]; if (f) _sysJarSetFile(f); }

function _sysJarSetFile(file) {
    if (!file.name.endsWith('.jar')) {
        const s = document.getElementById('sys-jar-status');
        if (s) { s.style.color = 'var(--red)'; s.innerHTML = '✗ Only <code>.jar</code> files accepted.'; }
        return;
    }
    _sysSelJar = file;
    const info  = document.getElementById('sys-jar-file-info');  if (info) info.style.display = 'block';
    const fname = document.getElementById('sys-jar-fname');       if (fname) fname.textContent = file.name;
    const fsize = document.getElementById('sys-jar-fsize');
    if (fsize) fsize.textContent = file.size > 1048576 ? (file.size/1048576).toFixed(1)+' MB' : (file.size/1024).toFixed(1)+' KB';
    const status = document.getElementById('sys-jar-status');     if (status) status.textContent = '';
}

function _sysJarClear() {
    _sysSelJar = null;
    const info  = document.getElementById('sys-jar-file-info'); if (info) info.style.display = 'none';
    const input = document.getElementById('sys-jar-input');      if (input) input.value = '';
}

async function _sysJarUpload() {
    if (!_sysSelJar) {
        const s = document.getElementById('sys-jar-status');
        if (s) { s.style.color = 'var(--red)'; s.textContent = '✗ Select a JAR file first.'; }
        return;
    }
    const status = document.getElementById('sys-jar-status');
    const jarName = _sysSelJar.name;
    const url     = window.location.origin + '/udf-jars/' + encodeURIComponent(jarName);
    const bytes   = await _sysSelJar.arrayBuffer();
    if (status) { status.style.color = 'var(--text2)'; status.textContent = `Uploading ${jarName}…`; }
    try {
        const r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/java-archive' }, body: bytes });
        if ([200, 201, 204].includes(r.status)) {
            _sysRecordJarUpload(jarName);
            const dockerCmd = `docker cp flink-studio:/var/www/udf-jars/${jarName} flink-jobmanager:/opt/flink/lib/\ndocker cp flink-studio:/var/www/udf-jars/${jarName} flink-taskmanager:/opt/flink/lib/`;
            if (status) {
                status.style.color = 'var(--green)';
                status.innerHTML = `✓ <strong>${jarName}</strong> uploaded to Studio.<br>
<span style="color:var(--text2);">Now copy to Flink and restart:</span>
<pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--green);border-radius:4px;padding:7px 10px;font-size:10px;font-family:var(--mono);color:var(--text1);white-space:pre;overflow-x:auto;margin:5px 0 0;">${escHtml(dockerCmd)}</pre>
<span style="color:var(--yellow);">Then click ⟳ Restart SQL Gateway session below.</span>`;
            }
            if (typeof addLog === 'function') addLog('OK', `Connector JAR uploaded: ${jarName}`);
            if (typeof toast  === 'function') toast(`${jarName} uploaded`, 'ok');
            _sysJarClear();
            setTimeout(() => { _sysJarLoadList(); _sysRefreshAvailability(); }, 500);
        } else {
            throw new Error(`HTTP ${r.status}`);
        }
    } catch(e) {
        if (status) { status.style.color = 'var(--red)'; status.textContent = `✗ Upload failed: ${e.message}`; }
    }
}

async function _sysJarLoadList() {
    const el = document.getElementById('sys-jar-list'); if (!el) return;
    try {
        const r = await fetch(window.location.origin + '/udf-jars/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name && f.name.endsWith('.jar')); } catch(_) {}
        if (!jars.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>'; return; }
        const fmtSz = b => b > 1048576 ? (b/1048576).toFixed(1)+' MB' : b > 1024 ? (b/1024).toFixed(1)+' KB' : b+' B';
        el.innerHTML = jars.map(j => `
      <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
        <span>📦</span>
        <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);">${escHtml(j.name)}</div>
        <span style="color:var(--text3);flex-shrink:0;">${j.size ? fmtSz(j.size) : '—'}</span>
        <button onclick="_sysJarDelete('${escHtml(j.name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button>
      </div>`).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">${e.message.includes('404') ? '/udf-jars/ not configured on the nginx proxy.' : escHtml(e.message)}</div>`;
    }
}

async function _sysJarDelete(name) {
    if (!confirm('Delete ' + name + ' from Studio container?')) return;
    const url = window.location.origin + '/udf-jars/' + encodeURIComponent(name);
    try {
        const r = await fetch(url, { method: 'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        if (typeof toast === 'function') toast(name + ' deleted', 'ok');
        _sysJarLoadList();
        setTimeout(_sysRefreshAvailability, 400);
    } catch(e) {
        if (typeof toast === 'function') toast('Delete failed: ' + e.message, 'err');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY RESTART / SESSION RECONNECT — restored from v1.1.0
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRestartGateway() {
    const st = document.getElementById('sys-restart-status');
    if (st) { st.style.color = 'var(--yellow)'; st.textContent = '⟳ Closing current Gateway session…'; }
    try {
        if (typeof state !== 'undefined' && state?.activeSession && state?.gateway) {
            const gwBase = _sysGatewayBase();
            try {
                await fetch(gwBase + '/flink-api/v1/sessions/' + state.activeSession, {
                    method: 'DELETE',
                    signal: AbortSignal.timeout(5000)
                });
            } catch(_) {}
        }
        if (st) st.textContent = '⟳ Waiting for Gateway to be ready…';
        await new Promise(r => setTimeout(r, 3000));
        if (st) st.textContent = '⟳ Opening new session…';
        if (typeof renewSession === 'function') {
            await renewSession();
            if (st) { st.style.color = 'var(--green)'; st.textContent = '✓ New session opened — connector JARs are now active. Close this panel and run your pipeline.'; }
            if (typeof toast    === 'function') toast('Gateway session restarted — JARs reloaded', 'ok');
            if (typeof addLog   === 'function') addLog('OK', 'Systems Manager: Gateway session restarted and new session opened.');
        } else if (typeof connectSession === 'function') {
            await connectSession();
            if (st) { st.style.color = 'var(--green)'; st.textContent = '✓ Reconnected — close this panel and continue.'; }
            if (typeof toast === 'function') toast('Reconnected to Gateway', 'ok');
        } else {
            if (st) { st.style.color = 'var(--yellow)'; st.textContent = '⚠ Session closed. Refresh the Studio page to reconnect.'; }
            if (typeof toast === 'function') toast('Session closed — refresh the page to reconnect', 'warn');
        }
    } catch(e) {
        if (st) { st.style.color = 'var(--red)'; st.textContent = '✗ Restart failed: ' + e.message; }
        if (typeof addLog === 'function') addLog('ERR', 'Systems Manager: Gateway restart failed: ' + e.message);
    }
}

async function _sysReconnectSession() {
    const st = document.getElementById('sys-restart-status');
    if (st) { st.style.color = 'var(--yellow)'; st.textContent = '⟳ Reconnecting…'; }
    try {
        if (typeof renewSession === 'function') {
            await renewSession();
            if (st) { st.style.color = 'var(--green)'; st.textContent = '✓ Reconnected successfully.'; }
            if (typeof toast    === 'function') toast('Studio session reconnected', 'ok');
            if (typeof addLog   === 'function') addLog('OK', 'Systems Manager: Studio session reconnected.');
        } else {
            if (st) { st.style.color = 'var(--yellow)'; st.textContent = '⚠ Reconnect function not available — refresh the page.'; }
        }
    } catch(e) {
        if (st) { st.style.color = 'var(--red)'; st.textContent = '✗ Reconnect failed: ' + e.message; }
    }
}