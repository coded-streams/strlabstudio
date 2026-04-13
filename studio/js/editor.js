// EDITOR
// ──────────────────────────────────────────────

// ── AUTOCOMPLETE DATA ──────────────────────────
const AC_KEYWORDS = [
  'SELECT','DISTINCT','FROM','WHERE','GROUP BY','HAVING','ORDER BY','LIMIT',
  'INSERT INTO','VALUES','UPDATE','SET','DELETE','CREATE TABLE','CREATE VIEW',
  'CREATE DATABASE','CREATE CATALOG','CREATE FUNCTION','CREATE TEMPORARY TABLE',
  'CREATE TEMPORARY VIEW','CREATE TEMPORARY FUNCTION','CREATE TEMPORARY SYSTEM FUNCTION',
  'DROP TABLE','DROP VIEW','DROP FUNCTION','ALTER TABLE',
  'DESCRIBE','EXPLAIN','SHOW TABLES','SHOW DATABASES','SHOW CATALOGS',
  'SHOW FUNCTIONS','SHOW USER FUNCTIONS','SHOW JOBS','USE CATALOG','USE',
  'UNION ALL','UNION','INTERSECT','EXCEPT',
  'LEFT JOIN','RIGHT JOIN','INNER JOIN','CROSS JOIN',
  'FULL OUTER JOIN','LEFT OUTER JOIN','JOIN','ON','AS','WITH','AND','OR',
  'NOT','IN','EXISTS','BETWEEN','LIKE','IS NULL','IS NOT NULL',
  'CASE','WHEN','THEN','ELSE','END','PARTITION BY','OVER','WINDOW',
  'WATERMARK FOR','INTERVAL','TIMESTAMP','CURRENT_TIMESTAMP','PROC_TIME()',
  'PRIMARY KEY','NOT ENFORCED','ENFORCED','EMIT','CHANGES',
  'TEMPORARY','SYSTEM','FUNCTION','CATALOG','DATABASE',
  'TABLE','DESCRIPTOR','IF NOT EXISTS','IF EXISTS',
  'ADD JAR','EXECUTE STATEMENT SET','BEGIN','END',
  'LANGUAGE SQL','RETURNS','AS $$'
];

const AC_FUNCTIONS = [
  'COUNT(','SUM(','AVG(','MIN(','MAX(','COALESCE(','NULLIF(','IF(',
  'CONCAT(','SUBSTRING(','TRIM(','UPPER(','LOWER(','LENGTH(','REPLACE(',
  'CAST(','TRY_CAST(','DATE_FORMAT(','UNIX_TIMESTAMP(','TO_TIMESTAMP(',
  'ROUND(','FLOOR(','CEIL(','ABS(','POWER(','MOD(',
  'TUMBLE(','HOP(','SESSION(','CUMULATE(',
  'ROW_NUMBER()','RANK()','DENSE_RANK()','LEAD(','LAG(',
  'FIRST_VALUE(','LAST_VALUE(','NTH_VALUE(',
  'REGEXP_EXTRACT(','REGEXP_REPLACE(','JSON_VALUE(','JSON_QUERY(',
  'ARRAY(','MAP(','ROW(','CARDINALITY(',
  'PROCTIME()','NOW()','CURRENT_DATE','CURRENT_TIME',
  'TO_DATE(','TO_TIME(','DATE_DIFF(',
];

const AC_TYPES = [
  'BIGINT','INT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(',
  'STRING','VARCHAR(','CHAR(','BOOLEAN','DATE','TIME','TIMESTAMP',
  'TIMESTAMP_LTZ','ARRAY<','MAP<','ROW<','BYTES'
];

const AC_SNIPPETS = [
  { label: 'SELECT *',             insert: 'SELECT *\nFROM ',                     detail: 'Basic select' },
  { label: 'SELECT ... FROM ... WHERE', insert: 'SELECT\n    *\nFROM\n    \nWHERE\n    ', detail: 'Select with filter' },
  { label: 'CREATE TABLE', insert:
        `CREATE TABLE \`table_name\` (
                                       \`col1\` STRING,
                                       \`col2\` BIGINT,
                                       \`ts\` TIMESTAMP(3),
                                       WATERMARK FOR \`ts\` AS \`ts\` - INTERVAL '5' SECOND
         ) WITH (
             'connector' = 'kafka',
             'topic' = '',
             'properties.bootstrap.servers' = ''
             );`, detail: 'Kafka source table' },
  { label: 'TUMBLE window', insert:
        `SELECT
           TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
           COUNT(*) AS cnt
         FROM
           source_table
         GROUP BY
           TUMBLE(ts, INTERVAL '1' MINUTE);`, detail: 'Tumble aggregation' },
  { label: 'HOP window', insert:
        `SELECT
    HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS cnt
FROM
    source_table
GROUP BY
    HOP(ts, INTERVAL '5' SECOND, INTERVAL '1' MINUTE);`, detail: 'Sliding window' },
  { label: 'JOIN two tables', insert:
        `SELECT
    a.id,
    a.name,
    b.value
FROM
    table_a AS a
INNER JOIN
    table_b AS b
ON
    a.id = b.id;`, detail: 'Inner join template' },
  { label: 'CASE WHEN', insert:
        `CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END`, detail: 'Conditional expression' },
  { label: 'WITH (CTE)', insert:
        `WITH cte_name AS (
          SELECT
            *
          FROM
            source_table
          WHERE
            condition
        )
         SELECT
           *
         FROM
           cte_name;`, detail: 'Common table expression' },
];

// ── HOVER TOOLTIP DOCS ─────────────────────────
// JavaDoc-style documentation for keywords and functions.
// Each entry: { signature, category, description, params[], returns, example, pipeline }
const DOCS = {

  // ── AGGREGATE FUNCTIONS ──────────────────────
  'COUNT': {
    signature: 'COUNT(expr)  /  COUNT(*)',
    category: 'Aggregate',
    description: 'Returns the number of rows for which the expression is not NULL. Use COUNT(*) to count all rows including NULLs.',
    params: [{ name: 'expr', desc: 'Column or expression to count. Use * to count all rows.' }],
    returns: 'BIGINT',
    example: "SELECT COUNT(*) AS total, COUNT(user_id) AS non_null_users\nFROM events;",
    pipeline: 'Commonly used with GROUP BY in streaming aggregations. Pair with TUMBLE() or HOP() for windowed counts over event streams.'
  },
  'SUM': {
    signature: 'SUM(expr)',
    category: 'Aggregate',
    description: 'Returns the sum of all non-NULL values in the group. Returns NULL if all values are NULL.',
    params: [{ name: 'expr', desc: 'Numeric column or expression to sum.' }],
    returns: 'BIGINT / DOUBLE (mirrors input type)',
    example: "SELECT SUM(amount) AS total_revenue\nFROM orders\nGROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);",
    pipeline: 'Use in streaming pipelines to compute running totals. Always define a watermark on the time column to trigger window emission.'
  },
  'AVG': {
    signature: 'AVG(expr)',
    category: 'Aggregate',
    description: 'Returns the average (arithmetic mean) of all non-NULL values in the group.',
    params: [{ name: 'expr', desc: 'Numeric column or expression.' }],
    returns: 'DOUBLE',
    example: "SELECT AVG(response_time_ms) AS avg_latency\nFROM api_logs\nGROUP BY endpoint, TUMBLE(ts, INTERVAL '5' MINUTE);",
    pipeline: 'Good for latency and metric dashboards in Flink streaming jobs. Combine with HOP windows for rolling averages.'
  },
  'MIN': {
    signature: 'MIN(expr)',
    category: 'Aggregate',
    description: 'Returns the minimum value among all non-NULL values in the group.',
    params: [{ name: 'expr', desc: 'Column or expression of any orderable type.' }],
    returns: 'Same type as input',
    example: "SELECT MIN(price) AS cheapest\nFROM product_events\nGROUP BY category;",
    pipeline: 'Use in windowed aggregations to track lowest value per window. Works on numeric, string, and timestamp types.'
  },
  'MAX': {
    signature: 'MAX(expr)',
    category: 'Aggregate',
    description: 'Returns the maximum value among all non-NULL values in the group.',
    params: [{ name: 'expr', desc: 'Column or expression of any orderable type.' }],
    returns: 'Same type as input',
    example: "SELECT MAX(bid_amount) AS winning_bid\nFROM auction_events\nGROUP BY auction_id;",
    pipeline: 'Useful for peak detection in event streams. Pair with a time window to avoid unbounded state.'
  },
  'COALESCE': {
    signature: 'COALESCE(expr1, expr2, ...)',
    category: 'Conditional',
    description: 'Returns the first non-NULL argument. Evaluates arguments left to right and returns as soon as a non-NULL is found.',
    params: [
      { name: 'expr1', desc: 'First value to evaluate.' },
      { name: 'expr2, ...', desc: 'Fallback values evaluated in order.' }
    ],
    returns: 'Same type as arguments',
    example: "SELECT COALESCE(display_name, username, 'Anonymous') AS name\nFROM users;",
    pipeline: 'Use to handle missing fields in Kafka messages where optional fields may be absent (arrive as NULL).'
  },
  'CAST': {
    signature: 'CAST(expr AS type)',
    category: 'Conversion',
    description: 'Converts a value to the specified data type. Throws a runtime error if the cast fails. Use TRY_CAST to suppress errors.',
    params: [
      { name: 'expr', desc: 'Value or column to convert.' },
      { name: 'type', desc: 'Target Flink SQL type (e.g. BIGINT, STRING, TIMESTAMP).' }
    ],
    returns: 'Specified target type',
    example: "SELECT CAST(event_time AS TIMESTAMP(3)) AS ts,\n       CAST(price_str AS DOUBLE) AS price\nFROM raw_events;",
    pipeline: 'Essential when ingesting raw STRING data from Kafka or filesystem sources that need type coercion before aggregation.'
  },
  'TRY_CAST': {
    signature: 'TRY_CAST(expr AS type)',
    category: 'Conversion',
    description: 'Attempts to cast the value to the specified type. Returns NULL instead of throwing an error if the cast fails. Safer than CAST for dirty data.',
    params: [
      { name: 'expr', desc: 'Value or column to convert.' },
      { name: 'type', desc: 'Target Flink SQL type.' }
    ],
    returns: 'Specified target type, or NULL on failure',
    example: "SELECT TRY_CAST(raw_amount AS DOUBLE) AS amount\nFROM payment_events\nWHERE TRY_CAST(raw_amount AS DOUBLE) IS NOT NULL;",
    pipeline: 'Recommended over CAST in production pipelines where upstream data quality cannot be guaranteed.'
  },
  'TUMBLE': {
    signature: 'TUMBLE(time_col, interval)',
    category: 'Window',
    description: 'Assigns each row to a fixed-size, non-overlapping time window (tumbling window). Each row belongs to exactly one window.',
    params: [
      { name: 'time_col', desc: 'The time attribute column (must have a WATERMARK defined).' },
      { name: 'interval', desc: "Window size as an INTERVAL literal, e.g. INTERVAL '1' MINUTE." }
    ],
    returns: 'Window grouping reference',
    example: "SELECT\n    TUMBLE_START(ts, INTERVAL '1' MINUTE) AS win_start,\n    COUNT(*) AS event_count\nFROM\n    clickstream\nGROUP BY\n    TUMBLE(ts, INTERVAL '1' MINUTE);",
    pipeline: "Ideal for batching stream data into fixed time buckets (e.g. per-minute metrics). The table must declare WATERMARK FOR ts AS ts - INTERVAL '5' SECOND to handle late events."
  },
  'HOP': {
    signature: "HOP(time_col, slide_interval, window_interval)",
    category: 'Window',
    description: 'Sliding (hopping) window. Each row can belong to multiple overlapping windows. Useful for rolling metrics.',
    params: [
      { name: 'time_col', desc: 'Time attribute with watermark.' },
      { name: 'slide_interval', desc: 'How often a new window starts.' },
      { name: 'window_interval', desc: 'Total size of each window.' }
    ],
    returns: 'Window grouping reference',
    example: "SELECT\n    HOP_START(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS win_start,\n    AVG(latency_ms) AS rolling_avg\nFROM\n    api_logs\nGROUP BY\n    HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);",
    pipeline: 'Use for rolling averages or moving sums. window_interval must be a multiple of slide_interval. Higher overlap = higher state size.'
  },
  'SESSION': {
    signature: "SESSION(time_col, gap_interval)",
    category: 'Window',
    description: 'Groups rows into sessions separated by a gap of inactivity. A new session starts when no event arrives within the gap interval.',
    params: [
      { name: 'time_col', desc: 'Time attribute with watermark.' },
      { name: 'gap_interval', desc: 'Maximum idle gap between events before a new session is created.' }
    ],
    returns: 'Window grouping reference',
    example: "SELECT\n    SESSION_START(ts, INTERVAL '30' MINUTE) AS session_start,\n    user_id,\n    COUNT(*) AS page_views\nFROM\n    user_activity\nGROUP BY\n    user_id, SESSION(ts, INTERVAL '30' MINUTE);",
    pipeline: 'User behaviour analysis — group page views per visit. Gap size depends on domain; 30 min is a common web analytics default.'
  },
  'WATERMARK': {
    signature: "WATERMARK FOR time_col AS time_col - INTERVAL 'n' unit",
    category: 'Table DDL',
    description: 'Declares a watermark strategy on a time attribute column. Watermarks tell Flink how late data can arrive and when to close windows.',
    params: [
      { name: 'time_col', desc: 'The TIMESTAMP(3) column used as the event time.' },
      { name: 'INTERVAL', desc: 'Allowed lateness (delay tolerance). Events arriving later than this are dropped.' }
    ],
    returns: 'DDL clause (used in CREATE TABLE)',
    example: "CREATE TABLE orders (\n    order_id STRING,\n    amount DOUBLE,\n    order_time TIMESTAMP(3),\n    WATERMARK FOR order_time AS order_time - INTERVAL '10' SECOND\n) WITH ( ... );",
    pipeline: "Always required on the source table when using event-time windows (TUMBLE, HOP, SESSION). Without a watermark, Flink cannot determine when to emit results."
  },
  'ROW_NUMBER': {
    signature: 'ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)',
    category: 'Window Function',
    description: 'Assigns a unique sequential integer to each row within a partition, ordered by the specified column. Starts at 1.',
    params: [],
    returns: 'BIGINT',
    example: "SELECT *\nFROM (\n    SELECT *,\n           ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn\n    FROM products\n)\nWHERE rn = 1;",
    pipeline: 'Use for deduplication or top-N patterns in Flink. The outer WHERE rn = 1 filter is the standard Flink pattern for keeping only the latest or highest-ranked row per key.'
  },
  'RANK': {
    signature: 'RANK() OVER (PARTITION BY ... ORDER BY ...)',
    category: 'Window Function',
    description: 'Assigns a rank to each row within a partition. Tied rows receive the same rank, and the next rank is skipped (gaps in ranking).',
    params: [],
    returns: 'BIGINT',
    example: "SELECT product_id, category,\n       RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS sales_rank\nFROM product_sales;",
    pipeline: 'Use for leaderboard-style queries. Note: RANK leaves gaps (1,1,3) while DENSE_RANK does not (1,1,2).'
  },
  'DENSE_RANK': {
    signature: 'DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)',
    category: 'Window Function',
    description: 'Like RANK() but without gaps — tied rows share a rank and the next rank increments by 1.',
    params: [],
    returns: 'BIGINT',
    example: "SELECT user_id,\n       DENSE_RANK() OVER (ORDER BY total_spend DESC) AS spend_rank\nFROM customer_spend;",
    pipeline: 'Preferred over RANK() when you need continuous ranking without gaps, e.g. customer tier assignment.'
  },
  'CONCAT': {
    signature: 'CONCAT(str1, str2, ...)',
    category: 'String',
    description: 'Concatenates two or more strings. Returns NULL if any argument is NULL. Use CONCAT_WS to handle NULLs gracefully.',
    params: [{ name: 'str1, str2, ...', desc: 'STRING values or expressions to join.' }],
    returns: 'STRING',
    example: "SELECT CONCAT(first_name, ' ', last_name) AS full_name\nFROM users;",
    pipeline: 'Useful for constructing composite keys or formatted messages before writing to a Kafka sink.'
  },
  'SUBSTRING': {
    signature: 'SUBSTRING(str FROM pos [FOR length])',
    category: 'String',
    description: 'Extracts a substring starting at position pos (1-based). Optional FOR clause limits the number of characters.',
    params: [
      { name: 'str', desc: 'Source STRING expression.' },
      { name: 'pos', desc: 'Start position (1-indexed).' },
      { name: 'length', desc: '(Optional) Number of characters to extract.' }
    ],
    returns: 'STRING',
    example: "SELECT SUBSTRING(order_id FROM 1 FOR 8) AS short_id\nFROM orders;",
    pipeline: 'Use when parsing structured string fields such as log lines, composite IDs, or fixed-format messages from Kafka.'
  },
  'REGEXP_EXTRACT': {
    signature: 'REGEXP_EXTRACT(str, pattern [, group])',
    category: 'String',
    description: 'Extracts a substring matching a Java regex pattern. Optionally specify a capture group index (default is 0 = full match).',
    params: [
      { name: 'str', desc: 'Input STRING.' },
      { name: 'pattern', desc: 'Java-compatible regular expression.' },
      { name: 'group', desc: '(Optional) Capture group index. 0 = whole match, 1 = first group.' }
    ],
    returns: 'STRING or NULL if no match',
    example: "SELECT REGEXP_EXTRACT(log_line, '\\\\bERROR\\\\s+(\\\\w+)', 1) AS error_code\nFROM app_logs;",
    pipeline: 'Essential for parsing unstructured log data from Kafka topics. Returns NULL for non-matching rows — filter or COALESCE as needed.'
  },
  'JSON_VALUE': {
    signature: "JSON_VALUE(json_str, path [RETURNING type] [DEFAULT val ON ERROR])",
    category: 'JSON',
    description: 'Extracts a scalar value from a JSON string using a JSONPath expression.',
    params: [
      { name: 'json_str', desc: 'STRING column containing valid JSON.' },
      { name: 'path', desc: "JSONPath expression, e.g. '$.user.id'." },
      { name: 'RETURNING', desc: '(Optional) Cast result to this type.' },
      { name: 'DEFAULT ... ON ERROR', desc: '(Optional) Fallback value on parse error.' }
    ],
    returns: 'STRING (or RETURNING type)',
    example: "SELECT JSON_VALUE(payload, '$.user.id') AS user_id,\n       JSON_VALUE(payload, '$.amount' RETURNING DOUBLE) AS amount\nFROM kafka_events;",
    pipeline: "The standard way to parse JSON Kafka messages in Flink SQL without defining a schema upfront. Use with 'format'='raw' or 'format'='json' connector."
  },
  'TO_TIMESTAMP': {
    signature: "TO_TIMESTAMP(str [, format])",
    category: 'Time',
    description: "Parses a STRING into a TIMESTAMP. Default format is 'yyyy-MM-dd HH:mm:ss'. Custom formats follow Java SimpleDateFormat patterns.",
    params: [
      { name: 'str', desc: 'STRING representation of a datetime.' },
      { name: 'format', desc: "(Optional) Java format pattern, e.g. 'yyyy-MM-dd'T'HH:mm:ss'." }
    ],
    returns: 'TIMESTAMP(3)',
    example: "SELECT TO_TIMESTAMP(event_time_str, 'yyyy-MM-dd''T''HH:mm:ss') AS event_ts\nFROM raw_events;",
    pipeline: "Use to convert string timestamps from Kafka messages into proper TIMESTAMP(3) columns before declaring them as watermark attributes."
  },
  'DATE_FORMAT': {
    signature: "DATE_FORMAT(timestamp, format)",
    category: 'Time',
    description: 'Formats a TIMESTAMP as a STRING using a Java SimpleDateFormat pattern.',
    params: [
      { name: 'timestamp', desc: 'TIMESTAMP or TIMESTAMP_LTZ column.' },
      { name: 'format', desc: "Java format string, e.g. 'yyyy-MM-dd HH:mm:ss'." }
    ],
    returns: 'STRING',
    example: "SELECT DATE_FORMAT(order_time, 'yyyy-MM-dd') AS order_date,\n       COUNT(*) AS daily_orders\nFROM orders\nGROUP BY DATE_FORMAT(order_time, 'yyyy-MM-dd');",
    pipeline: 'Use for grouping by date or hour without a formal window, or to format timestamps before writing to a JDBC / Elasticsearch sink.'
  },
  'CURRENT_TIMESTAMP': {
    signature: 'CURRENT_TIMESTAMP',
    category: 'Time',
    description: 'Returns the current processing-time timestamp (wall-clock time on the Flink TaskManager). Not deterministic — each invocation may differ.',
    params: [],
    returns: 'TIMESTAMP_LTZ(3)',
    example: "SELECT order_id, CURRENT_TIMESTAMP AS processed_at\nFROM orders;",
    pipeline: 'Use to stamp records with their processing time. Do NOT use as a watermark attribute — use actual event-time fields for windowing.'
  },
  'LEAD': {
    signature: 'LEAD(expr [, offset [, default]]) OVER (...)',
    category: 'Window Function',
    description: 'Returns the value of expr from a row that is offset rows ahead of the current row within the partition. Returns default (or NULL) if no such row exists.',
    params: [
      { name: 'expr', desc: 'Column or expression to look ahead on.' },
      { name: 'offset', desc: '(Optional) Number of rows to look ahead. Default is 1.' },
      { name: 'default', desc: '(Optional) Value returned when the offset row does not exist.' }
    ],
    returns: 'Same type as expr',
    example: "SELECT user_id, event_time,\n       LEAD(event_time, 1) OVER (PARTITION BY user_id ORDER BY event_time) AS next_event_time\nFROM user_events;",
    pipeline: 'Use to compute time-between-events or detect session transitions by comparing current and next row timestamps.'
  },
  'LAG': {
    signature: 'LAG(expr [, offset [, default]]) OVER (...)',
    category: 'Window Function',
    description: 'Returns the value of expr from a row that is offset rows behind the current row within the partition.',
    params: [
      { name: 'expr', desc: 'Column or expression to look behind on.' },
      { name: 'offset', desc: '(Optional) Number of rows to look back. Default is 1.' },
      { name: 'default', desc: '(Optional) Value returned when offset row does not exist.' }
    ],
    returns: 'Same type as expr',
    example: "SELECT order_id, amount,\n       LAG(amount, 1, 0.0) OVER (PARTITION BY user_id ORDER BY order_time) AS prev_amount\nFROM orders;",
    pipeline: 'Use for change detection, delta calculations, or state transitions in event streams.'
  },
  'SELECT': {
    signature: 'SELECT [DISTINCT] expr1, expr2, ... FROM ...',
    category: 'Query',
    description: 'Retrieves columns or computed expressions from one or more tables. The foundation of every Flink SQL query.',
    params: [
      { name: 'DISTINCT', desc: '(Optional) Eliminate duplicate rows from the result.' },
      { name: 'expr', desc: 'Column references, function calls, or expressions.' }
    ],
    returns: 'Result set',
    example: "SELECT user_id, COUNT(*) AS events\nFROM clickstream\nGROUP BY user_id;",
    pipeline: 'In streaming mode, SELECT runs continuously. Unbounded SELECT without aggregation writes every arriving event to the sink.'
  },
  'WHERE': {
    signature: 'WHERE condition',
    category: 'Query',
    description: 'Filters rows before grouping or aggregation. Only rows where condition evaluates to TRUE are kept.',
    params: [{ name: 'condition', desc: 'Boolean expression. Supports AND, OR, NOT, IN, BETWEEN, LIKE, IS NULL.' }],
    returns: 'Filtered row set',
    example: "SELECT * FROM orders\nWHERE status = 'PAID' AND amount > 100.0;",
    pipeline: 'Pushing filters close to the source reduces state and I/O. In Flink, WHERE pushdown is optimised automatically for supported connectors.'
  },
  'GROUP BY': {
    signature: 'GROUP BY expr1, expr2, ...',
    category: 'Query',
    description: 'Groups rows sharing the same values into summary rows. Must be combined with aggregate functions (COUNT, SUM, etc.).',
    params: [{ name: 'expr', desc: 'Column or expression to group by. In streaming, usually includes a time window function.' }],
    returns: 'One row per group',
    example: "SELECT region, SUM(revenue) AS total\nFROM sales\nGROUP BY region, TUMBLE(sale_time, INTERVAL '1' HOUR);",
    pipeline: 'Windowed GROUP BY is the core pattern of Flink streaming aggregation. Always include a window function in streaming GROUP BY to bound state.'
  },
  'JOIN': {
    signature: 'table1 JOIN table2 ON condition',
    category: 'Join',
    description: 'Combines rows from two tables where the join condition is true. In Flink streaming, both sides must be bounded or one must be a lookup/temporal table.',
    params: [
      { name: 'table1, table2', desc: 'Source tables or sub-queries.' },
      { name: 'condition', desc: 'Boolean expression linking the two sides.' }
    ],
    returns: 'Combined row set',
    example: "SELECT o.order_id, u.name, o.amount\nFROM orders AS o\nINNER JOIN users AS u\nON o.user_id = u.user_id;",
    pipeline: 'Regular stream-stream JOINs require both streams to have watermarks. For large state, prefer interval JOINs or lookup JOINs against a dimension table.'
  },
  'WITH': {
    signature: 'WITH cte_name AS (subquery) SELECT ...',
    category: 'Query',
    description: 'Defines a Common Table Expression (CTE) — a named temporary result set that can be referenced in the main query. Improves readability.',
    params: [
      { name: 'cte_name', desc: 'Alias for the sub-query.' },
      { name: 'subquery', desc: 'Any valid SELECT statement.' }
    ],
    returns: 'Inline view',
    example: "WITH recent_orders AS (\n    SELECT * FROM orders\n    WHERE order_time > CURRENT_TIMESTAMP - INTERVAL '1' DAY\n)\nSELECT user_id, COUNT(*) FROM recent_orders GROUP BY user_id;",
    pipeline: 'CTEs in Flink SQL are inlined — they are not materialised. Use them for clarity; for reuse across jobs, create a VIEW instead.'
  },
  'CASE': {
    signature: 'CASE WHEN cond THEN val [...] [ELSE default] END',
    category: 'Conditional',
    description: 'Evaluates conditions in order and returns the value for the first condition that is true. The ELSE clause handles unmatched rows.',
    params: [
      { name: 'WHEN cond', desc: 'Boolean condition to evaluate.' },
      { name: 'THEN val', desc: 'Value returned when condition is true.' },
      { name: 'ELSE default', desc: '(Optional) Fallback value if no condition matches.' }
    ],
    returns: 'Common type of all THEN/ELSE values',
    example: "SELECT order_id,\n    CASE\n        WHEN amount > 1000 THEN 'high'\n        WHEN amount > 100  THEN 'medium'\n        ELSE 'low'\n    END AS order_tier\nFROM orders;",
    pipeline: 'Use for bucketing or routing logic. In a pipeline, CASE is often used before writing to a partitioned sink to route records by tier.'
  },

  // ── MISSING SINGLE-WORD KEYWORDS ─────────────
  'FROM': {
    signature: 'FROM table_ref [, table_ref2]',
    category: 'Query',
    description: 'Specifies the table, view, or sub-query to read from. Multiple comma-separated tables produce a cross join; use explicit JOIN syntax for clarity.',
    params: [{ name: 'table_ref', desc: 'Table name, view name, or a sub-query wrapped in parentheses with an alias.' }],
    returns: 'Row source',
    example: "SELECT * FROM orders;\nSELECT * FROM (SELECT id, amt FROM orders WHERE amt > 0) AS t;",
    pipeline: 'The FROM clause binds the streaming source defined in CREATE TABLE. Flink reads continuously from it as events arrive.'
  },
  'DISTINCT': {
    signature: 'SELECT DISTINCT expr1, expr2, ...',
    category: 'Query',
    description: 'Eliminates duplicate rows from the result set. Two rows are duplicates when all selected column values are equal.',
    params: [{ name: 'expr', desc: 'Columns or expressions whose combined values define uniqueness.' }],
    returns: 'De-duplicated result set',
    example: "SELECT DISTINCT user_id FROM clickstream;",
    pipeline: 'In streaming, DISTINCT requires Flink to maintain state for every unique key seen so far. Use carefully — unbounded streams produce unbounded state. Prefer windowed deduplication with ROW_NUMBER() instead.'
  },
  'LIMIT': {
    signature: 'LIMIT n',
    category: 'Query',
    description: 'Restricts the number of rows returned by the query to at most n rows. Applied after all other clauses (WHERE, GROUP BY, ORDER BY).',
    params: [{ name: 'n', desc: 'Maximum number of rows to return. Must be a non-negative integer literal.' }],
    returns: 'At most n rows',
    example: "SELECT * FROM orders\nORDER BY order_time DESC\nLIMIT 10;",
    pipeline: 'In Flink streaming, LIMIT converts the job to a bounded query — it stops after emitting n rows. Combine with ORDER BY for top-N result patterns. For continuous top-N use ROW_NUMBER() with a filter instead.'
  },
  'HAVING': {
    signature: 'HAVING condition',
    category: 'Query',
    description: 'Filters groups produced by GROUP BY. Unlike WHERE (which filters rows before grouping), HAVING filters after aggregation.',
    params: [{ name: 'condition', desc: 'Boolean expression referencing aggregate results (e.g. COUNT(*) > 5).' }],
    returns: 'Filtered group set',
    example: "SELECT user_id, COUNT(*) AS event_count\nFROM clickstream\nGROUP BY user_id\nHAVING COUNT(*) > 100;",
    pipeline: 'Use HAVING to discard low-volume groups after windowed aggregation. This is applied per window emission in Flink streaming.'
  },
  'ORDER BY': {
    signature: 'ORDER BY expr [ASC | DESC] [, ...]',
    category: 'Query',
    description: 'Sorts the result set by one or more expressions. ASC (default) sorts ascending, DESC sorts descending.',
    params: [
      { name: 'expr', desc: 'Column name or expression to sort by.' },
      { name: 'ASC / DESC', desc: '(Optional) Sort direction. Default is ASC.' }
    ],
    returns: 'Ordered result set',
    example: "SELECT order_id, amount\nFROM orders\nORDER BY amount DESC, order_time ASC;",
    pipeline: 'In Flink streaming, ORDER BY on a time attribute is used to define processing order for certain operators. Sorting arbitrary non-time columns requires collecting all results first (bounded only).'
  },

  'UNION ALL': {
    signature: 'query1 UNION ALL query2',
    category: 'Set Operation',
    description: 'Combines the results of two queries, keeping all rows including duplicates. Both queries must have the same number of columns and compatible types.',
    params: [
      { name: 'query1', desc: 'First SELECT statement.' },
      { name: 'query2', desc: 'Second SELECT statement with matching column types.' }
    ],
    returns: 'Combined row set (with duplicates)',
    example: "SELECT user_id, 'mobile' AS source FROM mobile_events\nUNION ALL\nSELECT user_id, 'web' AS source FROM web_events;",
    pipeline: 'UNION ALL is the standard way to merge multiple Kafka topic streams in Flink SQL without deduplication overhead. Prefer over UNION when you know inputs have no duplicates or duplicates are intentional.'
  },
  'UNION': {
    signature: 'query1 UNION query2',
    category: 'Set Operation',
    description: 'Combines the results of two queries and removes duplicate rows. More expensive than UNION ALL as it requires a distinct pass.',
    params: [
      { name: 'query1', desc: 'First SELECT statement.' },
      { name: 'query2', desc: 'Second SELECT statement with matching column types.' }
    ],
    returns: 'Combined de-duplicated row set',
    example: "SELECT user_id FROM logins\nUNION\nSELECT user_id FROM registrations;",
    pipeline: 'In streaming, UNION maintains state for deduplication — use UNION ALL where possible. Only use UNION when deduplication is a hard requirement.'
  },
  'INTERSECT': {
    signature: 'query1 INTERSECT query2',
    category: 'Set Operation',
    description: 'Returns only rows that appear in both result sets. Eliminates duplicates.',
    params: [],
    returns: 'Rows common to both queries',
    example: "SELECT user_id FROM segment_a\nINTERSECT\nSELECT user_id FROM segment_b;",
    pipeline: 'Requires state to track both sides. In streaming Flink jobs, prefer a JOIN-based approach for large unbounded streams.'
  },
  'EXCEPT': {
    signature: 'query1 EXCEPT query2',
    category: 'Set Operation',
    description: 'Returns rows from query1 that do not appear in query2. Equivalent to set difference.',
    params: [],
    returns: 'Rows in query1 not in query2',
    example: "SELECT user_id FROM all_users\nEXCEPT\nSELECT user_id FROM churned_users;",
    pipeline: 'Produces unbounded state in streaming — use with caution on high-cardinality unbounded streams.'
  },
  'INNER JOIN': {
    signature: 'table1 INNER JOIN table2 ON condition',
    category: 'Join',
    description: 'Returns only rows where the join condition is satisfied on both sides. Non-matching rows are excluded.',
    params: [
      { name: 'table1, table2', desc: 'Source tables or sub-queries.' },
      { name: 'condition', desc: 'Equality or range expression linking the two sides.' }
    ],
    returns: 'Matched rows from both tables',
    example: "SELECT o.order_id, u.name\nFROM orders AS o\nINNER JOIN users AS u ON o.user_id = u.user_id;",
    pipeline: 'In Flink streaming, INNER JOIN between two streams keeps state for both sides indefinitely unless a time-bounded interval join condition is added.'
  },
  'LEFT JOIN': {
    signature: 'table1 LEFT JOIN table2 ON condition',
    category: 'Join',
    description: 'Returns all rows from the left table, and matched rows from the right table. Unmatched right-side columns are NULL.',
    params: [
      { name: 'table1', desc: 'Left (primary) table — all its rows are preserved.' },
      { name: 'table2', desc: 'Right table — only matching rows contribute.' }
    ],
    returns: 'All left rows, matched right columns or NULL',
    example: "SELECT o.order_id, u.name\nFROM orders AS o\nLEFT JOIN users AS u ON o.user_id = u.user_id;",
    pipeline: 'Useful when enriching an event stream with optional dimension data. In Flink, use a temporal table join for dimension lookup to avoid large join state.'
  },
  'RIGHT JOIN': {
    signature: 'table1 RIGHT JOIN table2 ON condition',
    category: 'Join',
    description: 'Returns all rows from the right table, and matched rows from the left. Unmatched left-side columns are NULL.',
    params: [],
    returns: 'All right rows, matched left columns or NULL',
    example: "SELECT o.order_id, u.name\nFROM orders AS o\nRIGHT JOIN users AS u ON o.user_id = u.user_id;",
    pipeline: 'Less common in streaming pipelines. Consider rewriting as a LEFT JOIN with tables swapped for clearer intent.'
  },
  'FULL OUTER JOIN': {
    signature: 'table1 FULL OUTER JOIN table2 ON condition',
    category: 'Join',
    description: 'Returns all rows from both tables. Unmatched rows from either side have NULL for the other side\'s columns.',
    params: [],
    returns: 'All rows from both tables, NULLs for unmatched sides',
    example: "SELECT a.id, b.id\nFROM stream_a AS a\nFULL OUTER JOIN stream_b AS b ON a.key = b.key;",
    pipeline: 'Generates large state in unbounded streaming. Use only when truly needed and pair with interval join conditions or watermarks to expire old state.'
  },
  'ON': {
    signature: 'JOIN ... ON condition',
    category: 'Join',
    description: 'Specifies the join condition between two tables. Evaluated as a boolean expression; rows where it is TRUE are matched.',
    params: [{ name: 'condition', desc: 'Boolean expression, typically an equality between keys from each table.' }],
    returns: 'Filter applied to joined rows',
    example: "SELECT * FROM orders AS o\nINNER JOIN users AS u\nON o.user_id = u.user_id AND o.region = u.region;",
    pipeline: 'Compound ON conditions (multiple AND clauses) can narrow join state significantly in streaming. Use equality on primary keys first, then refine with additional predicates.'
  },
  'AS': {
    signature: 'expr AS alias  /  table AS alias',
    category: 'Alias',
    description: 'Assigns an alias to a column expression or table reference. The alias is used in ORDER BY, HAVING, outer queries, and output schema.',
    params: [
      { name: 'expr', desc: 'Column, function call, or expression to rename.' },
      { name: 'alias', desc: 'New name. Use backtick quoting for reserved words: `count`.' }
    ],
    returns: 'Renamed output column or table reference',
    example: "SELECT COUNT(*) AS total_events, user_id AS uid\nFROM events AS e\nGROUP BY user_id;",
    pipeline: 'Column aliases defined in SELECT are used in the sink schema. Table aliases (FROM orders AS o) are required when self-joining or joining the same table multiple times.'
  },
  'AND': {
    signature: 'condition1 AND condition2',
    category: 'Logic',
    description: 'Returns TRUE only when both conditions are TRUE. Short-circuits: if the left side is FALSE, the right side is not evaluated.',
    params: [],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders\nWHERE status = 'PAID' AND amount > 100;",
    pipeline: 'Order AND conditions from most selective (narrowest filter) to least, to help the query planner push the tightest filter earliest.'
  },
  'OR': {
    signature: 'condition1 OR condition2',
    category: 'Logic',
    description: 'Returns TRUE when at least one condition is TRUE.',
    params: [],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders\nWHERE status = 'PENDING' OR status = 'RETRY';",
    pipeline: 'Consider rewriting OR on the same column as IN (...) for better readability and potential optimiser benefits.'
  },
  'NOT': {
    signature: 'NOT condition',
    category: 'Logic',
    description: 'Negates a boolean expression. Returns TRUE when the condition is FALSE, and FALSE when it is TRUE.',
    params: [{ name: 'condition', desc: 'Any boolean expression.' }],
    returns: 'BOOLEAN',
    example: "SELECT * FROM users WHERE NOT is_deleted;",
    pipeline: 'NOT can prevent index/predicate pushdown in some connectors. Prefer positive equality checks where possible.'
  },
  'IN': {
    signature: 'expr IN (value1, value2, ...)  /  expr IN (subquery)',
    category: 'Predicate',
    description: 'Returns TRUE if the expression matches any value in the list or sub-query result set.',
    params: [
      { name: 'expr', desc: 'Column or expression to test.' },
      { name: 'values / subquery', desc: 'Literal list or a SELECT returning a single column.' }
    ],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders WHERE status IN ('PAID', 'SHIPPED', 'DELIVERED');",
    pipeline: 'IN with a literal list is optimised to an OR chain. IN with a sub-query becomes a semi-join — ensure the sub-query is bounded or use a lookup table join instead.'
  },
  'EXISTS': {
    signature: 'EXISTS (subquery)',
    category: 'Predicate',
    description: 'Returns TRUE if the sub-query returns at least one row. Stops scanning as soon as one matching row is found.',
    params: [{ name: 'subquery', desc: 'A SELECT statement. Only its existence of rows matters, not the values.' }],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders AS o\nWHERE EXISTS (\n    SELECT 1 FROM payments AS p WHERE p.order_id = o.order_id\n);",
    pipeline: 'Translated to a semi-join internally. In streaming, requires state for both sides. Prefer explicit JOINs or lookup joins for better state control.'
  },
  'BETWEEN': {
    signature: 'expr BETWEEN low AND high',
    category: 'Predicate',
    description: 'Returns TRUE when expr is greater than or equal to low AND less than or equal to high. Inclusive on both ends.',
    params: [
      { name: 'expr', desc: 'Value to test.' },
      { name: 'low, high', desc: 'Lower and upper bounds (inclusive).' }
    ],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders WHERE amount BETWEEN 100.0 AND 500.0;",
    pipeline: 'Equivalent to expr >= low AND expr <= high. Useful for time range filters when combined with TO_TIMESTAMP().'
  },
  'LIKE': {
    signature: "expr LIKE pattern",
    category: 'Predicate',
    description: "Pattern match using SQL wildcards: % matches any sequence of characters, _ matches exactly one character.",
    params: [
      { name: 'expr', desc: 'STRING column or expression.' },
      { name: 'pattern', desc: "Pattern string. % = any chars, _ = one char. e.g. 'ERR%', '%timeout%'." }
    ],
    returns: 'BOOLEAN',
    example: "SELECT * FROM logs WHERE message LIKE '%ERROR%';",
    pipeline: 'LIKE with a leading % (e.g. LIKE \'%keyword\') cannot use index optimisations. For high-throughput streams, prefer REGEXP_EXTRACT or pre-parsed fields.'
  },
  'IS NULL': {
    signature: 'expr IS NULL',
    category: 'Predicate',
    description: 'Returns TRUE if the expression evaluates to NULL. Use instead of = NULL (which always returns NULL/FALSE).',
    params: [{ name: 'expr', desc: 'Any column or expression.' }],
    returns: 'BOOLEAN',
    example: "SELECT * FROM orders WHERE discount IS NULL;",
    pipeline: 'NULL checks are common when consuming Kafka messages with optional fields. Combine with COALESCE to substitute a default before writing to a sink.'
  },
  'IS NOT NULL': {
    signature: 'expr IS NOT NULL',
    category: 'Predicate',
    description: 'Returns TRUE if the expression is not NULL.',
    params: [{ name: 'expr', desc: 'Any column or expression.' }],
    returns: 'BOOLEAN',
    example: "SELECT * FROM events WHERE user_id IS NOT NULL;",
    pipeline: 'Use as an early filter to drop incomplete records before aggregation, reducing state size in streaming jobs.'
  },
  'WHEN': {
    signature: 'CASE WHEN condition THEN value ...',
    category: 'Conditional',
    description: 'Part of the CASE expression. Each WHEN clause specifies a condition; the first WHEN that evaluates to TRUE determines the result.',
    params: [
      { name: 'condition', desc: 'Boolean expression.' },
      { name: 'value', desc: 'Value to return when condition is TRUE (specified in the THEN clause).' }
    ],
    returns: 'See parent CASE expression',
    example: "CASE WHEN score >= 90 THEN 'A'\n     WHEN score >= 80 THEN 'B'\n     ELSE 'C'\nEND",
    pipeline: 'WHEN clauses are evaluated in order — place the most specific conditions first to avoid shadowing.'
  },
  'THEN': {
    signature: 'WHEN condition THEN value',
    category: 'Conditional',
    description: 'Paired with WHEN — specifies the value to return when the associated WHEN condition is TRUE.',
    params: [{ name: 'value', desc: 'Expression to return. Must be type-compatible with all other THEN/ELSE expressions.' }],
    returns: 'See parent CASE expression',
    example: "CASE WHEN active THEN 'enabled' ELSE 'disabled' END",
    pipeline: 'All THEN and ELSE values in a CASE expression must resolve to a common type. CAST explicitly if needed.'
  },
  'ELSE': {
    signature: 'CASE ... ELSE default_value END',
    category: 'Conditional',
    description: 'The fallback value in a CASE expression when no WHEN condition matches. If omitted, unmatched rows return NULL.',
    params: [{ name: 'default_value', desc: 'Value returned when no WHEN condition is TRUE.' }],
    returns: 'See parent CASE expression',
    example: "CASE WHEN type = 'A' THEN 1 ELSE 0 END AS is_type_a",
    pipeline: 'Always include an ELSE clause to make behaviour explicit. Omitting it returns NULL for unmatched rows, which can cause downstream aggregation issues.'
  },
  'END': {
    signature: 'CASE ... END',
    category: 'Conditional',
    description: 'Closes a CASE expression. Every CASE must have a matching END.',
    params: [],
    returns: 'Closes CASE block',
    example: "SELECT CASE WHEN amount > 0 THEN 'positive' ELSE 'zero' END AS sign\nFROM ledger;",
    pipeline: 'Missing END is a common syntax error. Ensure one END per CASE, including nested CASE expressions.'
  },
  'OVER': {
    signature: 'function() OVER (PARTITION BY ... ORDER BY ...)',
    category: 'Window Function',
    description: 'Defines the window frame over which a window function operates. Does not collapse rows like GROUP BY — each row retains its own output.',
    params: [
      { name: 'PARTITION BY', desc: '(Optional) Divides rows into independent partitions.' },
      { name: 'ORDER BY', desc: 'Defines the ordering within each partition.' }
    ],
    returns: 'Per-row window function result',
    example: "SELECT\n    user_id,\n    amount,\n    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_time) AS running_total\nFROM orders;",
    pipeline: 'OVER-based window functions maintain per-partition state in Flink. Unbounded preceding windows accumulate state indefinitely — bound them where possible.'
  },
  'PARTITION BY': {
    signature: 'PARTITION BY expr1 [, expr2, ...]',
    category: 'Window Function',
    description: 'Divides the rows into independent groups (partitions) for window function computation. Each partition is processed independently.',
    params: [{ name: 'expr', desc: 'Column or expression that defines partition boundaries.' }],
    returns: 'Partition grouping (used inside OVER clause)',
    example: "SELECT\n    user_id,\n    ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS rank\nFROM products;",
    pipeline: 'Choose high-cardinality PARTITION BY keys to distribute state across Flink TaskManagers evenly and avoid hot partitions.'
  },
  'INTERVAL': {
    signature: "INTERVAL 'n' unit",
    category: 'Time',
    description: "Represents a time duration. Used with watermark definitions, window sizes, and time arithmetic. Units: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR.",
    params: [
      { name: 'n', desc: 'Numeric literal specifying the duration.' },
      { name: 'unit', desc: 'Time unit: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR.' }
    ],
    returns: 'INTERVAL type',
    example: "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n\nGROUP BY TUMBLE(ts, INTERVAL '1' HOUR)\n\nWHERE order_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY",
    pipeline: "INTERVAL literals are required for window sizes in TUMBLE(), HOP(), SESSION(), and CUMULATE(). Always use string-quoted numbers: INTERVAL '1' MINUTE, not INTERVAL 1 MINUTE."
  },
  'TIMESTAMP': {
    signature: 'TIMESTAMP  /  TIMESTAMP(precision)',
    category: 'Type',
    description: 'A date-time type with optional fractional seconds precision (0–9). TIMESTAMP(3) is standard for millisecond event-time in Flink.',
    params: [{ name: 'precision', desc: '(Optional) Number of fractional second digits. Default is 0. Use 3 for milliseconds.' }],
    returns: 'TIMESTAMP type',
    example: "CREATE TABLE events (\n    event_id STRING,\n    event_time TIMESTAMP(3),\n    WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND\n) WITH ( ... );",
    pipeline: "TIMESTAMP(3) is the standard event-time column type in Flink. Use TIMESTAMP_LTZ(3) when your timestamps include timezone offset information."
  },
  'WINDOW': {
    signature: 'WINDOW w AS (PARTITION BY ... ORDER BY ...)',
    category: 'Window Function',
    description: 'Defines a named window specification that can be referenced by multiple window functions in the same SELECT, avoiding repetition.',
    params: [
      { name: 'w', desc: 'Window alias name.' },
      { name: 'PARTITION BY', desc: 'Partition definition.' },
      { name: 'ORDER BY', desc: 'Ordering within the partition.' }
    ],
    returns: 'Named window reference',
    example: "SELECT\n    user_id,\n    ROW_NUMBER() OVER w AS rn,\n    SUM(amount)  OVER w AS running_total\nFROM orders\nWINDOW w AS (PARTITION BY user_id ORDER BY order_time);",
    pipeline: 'Named windows reduce repetition and improve readability when multiple window functions share the same frame definition.'
  },
  'INSERT': {
    signature: 'INSERT INTO sink_table SELECT ...',
    category: 'DML',
    description: 'Writes query results into a target table (sink). In Flink SQL, this triggers continuous writing as new rows arrive from the source.',
    params: [
      { name: 'sink_table', desc: 'Target table previously defined with CREATE TABLE using a sink connector.' },
      { name: 'SELECT ...', desc: 'Any valid SELECT query whose output schema matches the sink.' }
    ],
    returns: 'N/A (write operation)',
    example: "INSERT INTO kafka_sink\nSELECT user_id, COUNT(*) AS events\nFROM clickstream\nGROUP BY user_id, TUMBLE(ts, INTERVAL '1' MINUTE);",
    pipeline: "The primary way to submit a streaming job in Flink SQL. Each INSERT INTO creates one Flink job. Use a StatementSet to combine multiple INSERT INTO statements into a single job to share source reads."
  },
  'VALUES': {
    signature: 'VALUES (val1, val2, ...) [, (val3, ...)]',
    category: 'DML',
    description: 'Provides inline literal rows, typically used with INSERT INTO or as an inline table in a FROM clause.',
    params: [{ name: 'val', desc: 'Literal values. Row count and types must match the target schema.' }],
    returns: 'Inline row set',
    example: "INSERT INTO config_table (key, value)\nVALUES ('timeout', '30'), ('retries', '3');",
    pipeline: 'Primarily used for testing and seeding lookup tables. Not suitable for high-throughput streaming data ingestion.'
  },
  'CREATE TABLE': {
    signature: 'CREATE TABLE name ( columns ) WITH ( connector_options )',
    category: 'DDL',
    description: 'Defines a new table backed by an external connector (Kafka, JDBC, filesystem, etc.). Does not physically create data storage — it registers a logical schema mapping.',
    params: [
      { name: 'name', desc: 'Table identifier. Use backtick quoting for reserved words.' },
      { name: 'columns', desc: 'Column definitions with types. Include WATERMARK FOR ... for event-time tables.' },
      { name: 'WITH', desc: "Connector-specific options as key-value pairs e.g. 'connector' = 'kafka'." }
    ],
    returns: 'N/A (DDL)',
    example: "CREATE TABLE orders (\n    order_id STRING,\n    amount   DOUBLE,\n    ts       TIMESTAMP(3),\n    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n    'connector' = 'kafka',\n    'topic'     = 'orders',\n    'properties.bootstrap.servers' = 'localhost:9092',\n    'format'    = 'json'\n);",
    pipeline: "Always define event-time watermarks on streaming source tables. For sink tables, omit the WATERMARK clause and use write-specific connector options like 'sink.parallelism'."
  },
  'CREATE VIEW': {
    signature: 'CREATE VIEW view_name AS SELECT ...',
    category: 'DDL',
    description: 'Creates a named virtual table from a SELECT statement. The view is not materialised — it is re-evaluated each time it is referenced.',
    params: [
      { name: 'view_name', desc: 'Name for the view.' },
      { name: 'SELECT ...', desc: 'Defining query.' }
    ],
    returns: 'N/A (DDL)',
    example: "CREATE VIEW active_users AS\nSELECT user_id, MAX(event_time) AS last_seen\nFROM events\nGROUP BY user_id;",
    pipeline: 'Use views to modularise complex pipelines. In Flink SQL scripts, views defined earlier can be referenced by later INSERT INTO statements in the same session.'
  },
  'DROP TABLE': {
    signature: 'DROP TABLE [IF EXISTS] table_name',
    category: 'DDL',
    description: 'Removes a table definition from the catalog. Does not delete data from the underlying connector — only the logical registration is removed.',
    params: [
      { name: 'IF EXISTS', desc: '(Optional) Suppresses error if the table does not exist.' },
      { name: 'table_name', desc: 'Name of the table to drop.' }
    ],
    returns: 'N/A (DDL)',
    example: "DROP TABLE IF EXISTS stale_source;",
    pipeline: 'Safe to use in development to clean up table registrations. In production, dropping a source table while a job reads from it will fail the job.'
  },
  'ALTER TABLE': {
    signature: 'ALTER TABLE table_name ...',
    category: 'DDL',
    description: 'Modifies an existing table definition — e.g. renaming it, adding columns, or changing connector properties.',
    params: [{ name: 'table_name', desc: 'Existing table to modify.' }],
    returns: 'N/A (DDL)',
    example: "ALTER TABLE orders RENAME TO orders_v2;",
    pipeline: 'ALTER TABLE support varies by catalog. Hive catalog supports more DDL mutations than the in-memory catalog. Changing connector properties usually requires recreating the table.'
  },


  'USE CATALOG': {
    signature: 'USE CATALOG catalog_name',
    category: 'Utility',
    description: 'Switches the active catalog. Subsequent table references are resolved against the new catalog.',
    params: [{ name: 'catalog_name', desc: 'Name of a registered catalog (e.g. hive, iceberg, default_catalog).' }],
    returns: 'N/A',
    example: "USE CATALOG hive_catalog;\nSHOW DATABASES;",
    pipeline: "Register custom catalogs in flink-conf.yaml or via CREATE CATALOG before using USE CATALOG. The default catalog is 'default_catalog'."
  },

  // ── WINDOW TABLE FUNCTIONS ───────────────────

  'TUMBLE_START': {
    signature: "TUMBLE_START(time_col, INTERVAL 'n' unit)",
    category: 'Window TVF',
    description: 'Returns the inclusive start timestamp of the TUMBLE window that contains the row. Use in SELECT to label the window.',
    params: [
      { name: 'time_col', desc: 'Same time attribute used in the GROUP BY TUMBLE(...).' },
      { name: "INTERVAL 'n' unit", desc: 'Same interval as in the TUMBLE() call.' }
    ],
    returns: 'TIMESTAMP — window start (inclusive)',
    example: "SELECT\n    TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,\n    TUMBLE_END(ts, INTERVAL '5' MINUTE)   AS window_end,\n    SUM(amount) AS total\nFROM orders\nGROUP BY TUMBLE(ts, INTERVAL '5' MINUTE);",
    pipeline: 'Pair TUMBLE_START with TUMBLE_END to label window boundaries in the sink. Both must use the same interval as TUMBLE() in GROUP BY.'
  },
  'TUMBLE_END': {
    signature: "TUMBLE_END(time_col, INTERVAL 'n' unit)",
    category: 'Window TVF',
    description: 'Returns the exclusive end timestamp of the TUMBLE window. The actual window covers [TUMBLE_START, TUMBLE_END).',
    params: [
      { name: 'time_col', desc: 'Same time attribute used in GROUP BY TUMBLE(...).' },
      { name: "INTERVAL 'n' unit", desc: 'Same interval as in the TUMBLE() call.' }
    ],
    returns: 'TIMESTAMP — window end (exclusive)',
    example: "SELECT TUMBLE_START(ts, INTERVAL '1' HOUR) AS start,\n       TUMBLE_END(ts,   INTERVAL '1' HOUR) AS end,\n       COUNT(*) AS events\nFROM events\nGROUP BY TUMBLE(ts, INTERVAL '1' HOUR);",
    pipeline: 'Use TUMBLE_END as the timestamp for the sink record to represent when the window closed.'
  },

  'HOP_START': {
    signature: "HOP_START(time_col, INTERVAL 'slide' unit, INTERVAL 'size' unit)",
    category: 'Window TVF',
    description: 'Returns the inclusive start timestamp of the HOP window for the current row.',
    params: [],
    returns: 'TIMESTAMP — window start (inclusive)',
    example: "SELECT HOP_START(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS start,\n       COUNT(*) AS cnt\nFROM events\nGROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);",
    pipeline: 'Use alongside HOP_END to label the window boundaries written to the sink.'
  },
  'HOP_END': {
    signature: "HOP_END(time_col, INTERVAL 'slide' unit, INTERVAL 'size' unit)",
    category: 'Window TVF',
    description: 'Returns the exclusive end timestamp of the HOP window for the current row.',
    params: [],
    returns: 'TIMESTAMP — window end (exclusive)',
    example: "SELECT HOP_END(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS end,\n       SUM(revenue) AS total\nFROM sales\nGROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);",
    pipeline: 'Pair with HOP_START for complete window interval labelling in the output.'
  },

  'SESSION_START': {
    signature: "SESSION_START(time_col, INTERVAL 'gap' unit)",
    category: 'Window TVF',
    description: 'Returns the start timestamp of the SESSION window.',
    params: [],
    returns: 'TIMESTAMP — session start',
    example: "SELECT user_id,\n       SESSION_START(ts, INTERVAL '10' MINUTE) AS start,\n       SESSION_END(ts, INTERVAL '10' MINUTE)   AS end\nFROM events\nGROUP BY user_id, SESSION(ts, INTERVAL '10' MINUTE);",
    pipeline: 'Use to calculate session duration: SESSION_END - SESSION_START gives the elapsed time per session.'
  },
  'SESSION_END': {
    signature: "SESSION_END(time_col, INTERVAL 'gap' unit)",
    category: 'Window TVF',
    description: 'Returns the exclusive end timestamp of the SESSION window.',
    params: [],
    returns: 'TIMESTAMP — session end (exclusive)',
    example: "SELECT SESSION_END(ts, INTERVAL '30' MINUTE) AS session_end\nFROM user_activity\nGROUP BY user_id, SESSION(ts, INTERVAL '30' MINUTE);",
    pipeline: 'The session ends when no events arrive within the gap. SESSION_END marks the moment the last event\'s gap expired.'
  },
  'CUMULATE': {
    signature: "CUMULATE(time_col, INTERVAL 'step' unit, INTERVAL 'max' unit)",
    category: 'Window TVF',
    description: 'Cumulating window — emits intermediate results at regular step intervals within a larger max window. Results accumulate until the max window closes.',
    params: [
      { name: 'time_col', desc: 'Time attribute with watermark.' },
      { name: 'step interval', desc: 'Frequency of intermediate emissions.' },
      { name: 'max interval', desc: 'Total accumulation window size. Must be a multiple of step.' }
    ],
    returns: 'Window grouping reference',
    example: "SELECT\n    CUMULATE_START(ts, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) AS win_start,\n    COUNT(*) AS running_count\nFROM events\nGROUP BY CUMULATE(ts, INTERVAL '1' MINUTE, INTERVAL '1' HOUR);",
    pipeline: "Useful for real-time dashboards that show 'count so far this hour' updated every minute. Introduced in Flink 1.13 as a Table Valued Function (TVF)."
  },

  // ── STRING FUNCTIONS ─────────────────────────
  'NULLIF': {
    signature: 'NULLIF(expr1, expr2)',
    category: 'Conditional',
    description: 'Returns NULL if expr1 equals expr2, otherwise returns expr1. Useful to convert sentinel values (like 0 or empty string) to NULL.',
    params: [
      { name: 'expr1', desc: 'Value to return (unless equal to expr2).' },
      { name: 'expr2', desc: 'Value to compare against. If equal to expr1, NULL is returned.' }
    ],
    returns: 'Same type as expr1, or NULL',
    example: "SELECT NULLIF(error_code, 0) AS error  -- returns NULL when error_code is 0\nFROM events;",
    pipeline: 'Combine with COALESCE to normalise sentinel/placeholder values in incoming Kafka messages before aggregation.'
  },
  'IF': {
    signature: 'IF(condition, true_value, false_value)',
    category: 'Conditional',
    description: 'Returns true_value if the condition is TRUE, otherwise returns false_value. Shorthand for a simple two-branch CASE WHEN.',
    params: [
      { name: 'condition', desc: 'Boolean expression.' },
      { name: 'true_value', desc: 'Returned when condition is TRUE.' },
      { name: 'false_value', desc: 'Returned when condition is FALSE or NULL.' }
    ],
    returns: 'Common type of true_value and false_value',
    example: "SELECT IF(amount > 0, 'credit', 'debit') AS tx_type\nFROM transactions;",
    pipeline: 'Simpler than CASE WHEN for binary conditions. Type compatibility between true_value and false_value is enforced at planning time.'
  },
  'TRIM': {
    signature: 'TRIM([LEADING | TRAILING | BOTH] [char FROM] str)',
    category: 'String',
    description: 'Removes leading and/or trailing characters from a string. Default removes spaces from both ends.',
    params: [
      { name: 'LEADING / TRAILING / BOTH', desc: '(Optional) Which end(s) to trim. Default: BOTH.' },
      { name: 'char', desc: '(Optional) Character to remove. Default: space.' },
      { name: 'str', desc: 'Source STRING.' }
    ],
    returns: 'STRING',
    example: "SELECT TRIM(BOTH ' ' FROM '  hello  ') AS cleaned;  -- 'hello'\nSELECT TRIM(LEADING '0' FROM order_id) AS trimmed_id FROM orders;",
    pipeline: 'Use to clean whitespace from raw string fields ingested from CSV or text-format Kafka messages.'
  },
  'UPPER': {
    signature: 'UPPER(str)',
    category: 'String',
    description: 'Converts all characters in the string to uppercase.',
    params: [{ name: 'str', desc: 'STRING expression.' }],
    returns: 'STRING',
    example: "SELECT UPPER(country_code) AS code FROM users;",
    pipeline: 'Useful for normalising case-inconsistent data before GROUP BY or JOIN operations.'
  },
  'LOWER': {
    signature: 'LOWER(str)',
    category: 'String',
    description: 'Converts all characters in the string to lowercase.',
    params: [{ name: 'str', desc: 'STRING expression.' }],
    returns: 'STRING',
    example: "SELECT LOWER(email) AS email FROM users;",
    pipeline: 'Normalise email or username fields to lowercase before deduplication or lookup joins.'
  },
  'LENGTH': {
    signature: 'LENGTH(str)  /  CHAR_LENGTH(str)',
    category: 'String',
    description: 'Returns the number of characters (not bytes) in the string.',
    params: [{ name: 'str', desc: 'STRING expression.' }],
    returns: 'INT',
    example: "SELECT * FROM messages WHERE LENGTH(body) > 256;",
    pipeline: 'Use to filter oversized payloads before writing to bounded-length sink columns (e.g. JDBC VARCHAR fields).'
  },
  'REPLACE': {
    signature: 'REPLACE(str, search, replacement)',
    category: 'String',
    description: 'Replaces all occurrences of the search string with the replacement string.',
    params: [
      { name: 'str', desc: 'Source STRING.' },
      { name: 'search', desc: 'Substring to find.' },
      { name: 'replacement', desc: 'Substring to substitute.' }
    ],
    returns: 'STRING',
    example: "SELECT REPLACE(phone_number, '-', '') AS digits_only FROM contacts;",
    pipeline: 'Use for lightweight string normalisation (remove separators, redact tokens) before writing to a sink or comparing across sources.'
  },
  'REGEXP_REPLACE': {
    signature: 'REGEXP_REPLACE(str, pattern, replacement)',
    category: 'String',
    description: 'Replaces all substrings matching the Java regex pattern with the replacement string.',
    params: [
      { name: 'str', desc: 'Source STRING.' },
      { name: 'pattern', desc: 'Java regular expression.' },
      { name: 'replacement', desc: 'Replacement string. Use $1, $2 for capture groups.' }
    ],
    returns: 'STRING',
    example: "SELECT REGEXP_REPLACE(log_line, '\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}', '[IP]') AS masked\nFROM access_logs;",
    pipeline: 'Use for PII masking (redact IPs, emails, phone numbers) in streaming pipelines before writing to a non-secure sink.'
  },
  'UNIX_TIMESTAMP': {
    signature: "UNIX_TIMESTAMP()  /  UNIX_TIMESTAMP(str [, format])",
    category: 'Time',
    description: "Returns the current Unix epoch seconds, or converts a formatted datetime string to epoch seconds.",
    params: [
      { name: 'str', desc: "(Optional) Datetime string to parse." },
      { name: 'format', desc: "(Optional) Java SimpleDateFormat pattern. Default: 'yyyy-MM-dd HH:mm:ss'." }
    ],
    returns: 'BIGINT (seconds since 1970-01-01 00:00:00 UTC)',
    example: "SELECT UNIX_TIMESTAMP() AS now_epoch;\nSELECT UNIX_TIMESTAMP(event_time_str) AS epoch FROM raw_events;",
    pipeline: 'Useful when writing to sinks that store timestamps as epoch integers. For event-time windowing, prefer TO_TIMESTAMP() to get a proper TIMESTAMP(3) attribute.'
  },
  'JSON_QUERY': {
    signature: "JSON_QUERY(json_str, path [RETURNING type] [WITH wrapper])",
    category: 'JSON',
    description: 'Extracts a JSON object or array from a JSON string using JSONPath. Unlike JSON_VALUE, this can return non-scalar results.',
    params: [
      { name: 'json_str', desc: 'STRING column containing valid JSON.' },
      { name: 'path', desc: "JSONPath expression e.g. '$.items'." },
      { name: 'WITH wrapper', desc: '(Optional) WITH ARRAY WRAPPER / WITHOUT ARRAY WRAPPER.' }
    ],
    returns: 'STRING (JSON fragment)',
    example: "SELECT JSON_QUERY(payload, '$.tags') AS tags_json\nFROM events;",
    pipeline: 'Use JSON_QUERY to extract nested JSON objects or arrays, then parse the result with a second JSON_VALUE call or a downstream process.'
  },
  'FIRST_VALUE': {
    signature: 'FIRST_VALUE(expr) OVER (...)',
    category: 'Window Function',
    description: 'Returns the first value of expr in the ordered window frame.',
    params: [{ name: 'expr', desc: 'Column or expression to evaluate.' }],
    returns: 'Same type as expr',
    example: "SELECT\n    user_id,\n    FIRST_VALUE(page) OVER (PARTITION BY session_id ORDER BY ts) AS landing_page\nFROM page_views;",
    pipeline: 'Use to track the entry point of a user session (landing page, first product viewed). Combine with ROW_NUMBER to materialise per-session first events.'
  },
  'LAST_VALUE': {
    signature: 'LAST_VALUE(expr) OVER (...)',
    category: 'Window Function',
    description: 'Returns the last value of expr in the ordered window frame.',
    params: [{ name: 'expr', desc: 'Column or expression to evaluate.' }],
    returns: 'Same type as expr',
    example: "SELECT\n    user_id,\n    LAST_VALUE(status) OVER (PARTITION BY order_id ORDER BY updated_at) AS current_status\nFROM order_updates;",
    pipeline: 'Use for last-known-value patterns (latest order status, most recent location). In Flink, this can produce large state — set appropriate state TTL.'
  },
  'NTH_VALUE': {
    signature: 'NTH_VALUE(expr, n) OVER (...)',
    category: 'Window Function',
    description: 'Returns the value of expr at the nth row in the ordered window frame (1-indexed).',
    params: [
      { name: 'expr', desc: 'Column or expression.' },
      { name: 'n', desc: 'Row position within the window (1 = first row).' }
    ],
    returns: 'Same type as expr, or NULL if fewer than n rows',
    example: "SELECT\n    user_id,\n    NTH_VALUE(product_id, 2) OVER (PARTITION BY user_id ORDER BY purchase_time) AS second_purchase\nFROM purchases;",
    pipeline: 'Use for sequence-aware analytics, e.g. second product purchased, third event in a session.'
  },
  'CARDINALITY': {
    signature: 'CARDINALITY(collection)',
    category: 'Collection',
    description: 'Returns the number of elements in an ARRAY or the number of entries in a MAP.',
    params: [{ name: 'collection', desc: 'An ARRAY<T> or MAP<K,V> column.' }],
    returns: 'INT',
    example: "SELECT order_id, CARDINALITY(items) AS item_count FROM orders;",
    pipeline: 'Use to filter or aggregate based on array/map size, e.g. orders with more than 5 line items.'
  },
  'PRIMARY KEY': {
    signature: 'PRIMARY KEY (col1 [, col2]) NOT ENFORCED',
    category: 'DDL Constraint',
    description: 'Declares the primary key columns of a table. In Flink SQL, constraints are NOT ENFORCED — they are metadata hints for the optimiser and upsert mode sinks.',
    params: [
      { name: 'col1, col2', desc: 'Column(s) forming the primary key.' },
      { name: 'NOT ENFORCED', desc: 'Required keyword — Flink does not validate uniqueness at runtime.' }
    ],
    returns: 'N/A (DDL)',
    example: "CREATE TABLE user_profiles (\n    user_id STRING,\n    name    STRING,\n    email   STRING,\n    PRIMARY KEY (user_id) NOT ENFORCED\n) WITH (\n    'connector' = 'upsert-kafka',\n    'topic'     = 'profiles',\n    'key.format' = 'raw',\n    'value.format' = 'json',\n    'properties.bootstrap.servers' = 'localhost:9092'\n);",
    pipeline: "Required for upsert-mode connectors (upsert-kafka, JDBC upsert). The primary key determines the upsert key used when writing. Without it, the connector operates in append mode only."
  },

  // ── DDL & SYSTEM KEYWORDS ──────────────────────
  'SET': {
    signature: "SET 'key' = 'value'",
    category: 'Configuration',
    description: "Sets a Flink SQL session property. Properties control runtime mode, parallelism, checkpointing, state TTL, and more. Must be executed before the query that uses the setting.",
    params: [
      { name: "'key'",  desc: "Flink property name in single quotes, e.g. 'execution.runtime-mode'." },
      { name: "'value'",desc: "Property value in single quotes." }
    ],
    returns: 'N/A',
    example: "SET 'execution.runtime-mode' = 'streaming';\nSET 'parallelism.default' = '4';\nSET 'execution.checkpointing.interval' = '10000';\nSET 'table.exec.state.ttl' = '3600000';",
    pipeline: "Always place SET statements at the top of your script before CREATE TABLE or INSERT INTO. They are session-scoped and persist for the lifetime of the session."
  },
  'TEMPORARY': {
    signature: 'CREATE TEMPORARY TABLE | VIEW | FUNCTION ...',
    category: 'DDL Modifier',
    description: "Marks a table, view, or function as session-scoped. Temporary objects are visible only within the current session and are automatically dropped when the session ends. They take precedence over permanent catalog objects with the same name.",
    params: [],
    returns: 'N/A (modifier keyword)',
    example: "CREATE TEMPORARY TABLE datagen_src (\n    id BIGINT,\n    amount DOUBLE\n) WITH ('connector' = 'datagen');\n\nCREATE TEMPORARY VIEW active_users AS\n    SELECT * FROM users WHERE active = TRUE;",
    pipeline: "Use TEMPORARY for sources and sinks in development to avoid polluting the persistent catalog. Production pipelines should use permanent tables registered in a Hive or Iceberg catalog."
  },
  'DROP': {
    signature: 'DROP [TEMPORARY] TABLE | VIEW | FUNCTION [IF EXISTS] name',
    category: 'DDL',
    description: "Removes a table, view, or function definition from the current catalog and database. For TEMPORARY objects, removes from the session. IF EXISTS prevents an error if the object does not exist.",
    params: [
      { name: 'TEMPORARY', desc: '(Optional) Drop only a session-scoped temporary object.' },
      { name: 'IF EXISTS', desc: '(Optional) Suppresses error when the object is not found.' },
      { name: 'name',      desc: 'Fully qualified or simple name of the object to remove.' }
    ],
    returns: 'N/A (DDL)',
    example: "DROP TABLE IF EXISTS stale_source;\nDROP TEMPORARY VIEW IF EXISTS temp_view;\nDROP FUNCTION IF EXISTS my_classifier;",
    pipeline: "Safe to use in development scripts to clean up before re-running. In production, dropping a source table while a job reads from it will immediately fail the streaming job."
  },
  'FUNCTION': {
    signature: 'CREATE [TEMPORARY] [SYSTEM] FUNCTION [IF NOT EXISTS] name AS class_name [USING JAR ...]',
    category: 'UDF',
    description: "Registers a custom function (UDF). The function can be a ScalarFunction, TableFunction, AggregateFunction, or async variant. TEMPORARY functions are session-scoped. SYSTEM functions are visible globally across all sessions and catalogs.",
    params: [
      { name: 'TEMPORARY', desc: '(Optional) Register for current session only.' },
      { name: 'SYSTEM',    desc: '(Optional) Register as a system-level function visible globally.' },
      { name: 'name',      desc: 'Function name used in SQL queries.' },
      { name: 'class_name',desc: 'Fully qualified Java class implementing the UDF.' },
      { name: 'USING JAR', desc: "(Optional) Path to JAR file: USING JAR '/opt/flink/lib/my.jar'." }
    ],
    returns: 'N/A (DDL)',
    example: "-- SQL UDF (no JAR needed)\nCREATE TEMPORARY FUNCTION risk_tier(score DOUBLE)\nRETURNS STRING LANGUAGE SQL AS $$\n    CASE WHEN score >= 0.8 THEN 'HIGH'\n         WHEN score >= 0.5 THEN 'MED'\n         ELSE 'LOW' END\n$$;\n\n-- Java UDF\nCREATE TEMPORARY FUNCTION fraud_score\nAS 'com.example.FraudScoreFunction'\nUSING JAR '/opt/flink/lib/fraud-udf.jar';",
    pipeline: "SQL UDFs execute entirely inside Flink SQL — no JAR needed. Java/Python UDFs require the JAR on the Flink classpath (use ADD JAR or the UDF Manager upload). Call the function by name in any SELECT: fraud_score(amount)."
  },
  'SYSTEM': {
    signature: 'CREATE TEMPORARY SYSTEM FUNCTION name AS ...',
    category: 'UDF Modifier',
    description: "Makes a TEMPORARY FUNCTION visible in the system namespace — callable from any catalog or database without prefix. Used for cross-catalog UDFs that need to be accessible everywhere in the session.",
    params: [],
    returns: 'N/A (modifier keyword)',
    example: "CREATE TEMPORARY SYSTEM FUNCTION classify\nAS 'com.example.ClassifyFunction'\nUSING JAR '/opt/flink/lib/utils.jar';\n\n-- Now callable without catalog prefix in any database:\nSELECT classify(event_type) FROM events;",
    pipeline: "SYSTEM functions shadow built-in functions of the same name — be careful with naming. For most use cases, non-SYSTEM TEMPORARY functions are sufficient."
  },
  'CREATE': {
    signature: 'CREATE [TEMPORARY] TABLE | VIEW | FUNCTION | CATALOG | DATABASE ...',
    category: 'DDL',
    description: "Defines a new object in the current catalog/database. The type of object (TABLE, VIEW, FUNCTION, CATALOG, DATABASE) determines what follows. In Flink SQL, CREATE does not physically create storage — it registers a logical definition.",
    params: [
      { name: 'TABLE',    desc: 'Register a source or sink backed by an external connector.' },
      { name: 'VIEW',     desc: 'Define a named virtual query (not materialised).' },
      { name: 'FUNCTION', desc: 'Register a custom UDF (scalar, table, or aggregate).' },
      { name: 'CATALOG',  desc: 'Register an external catalog (Hive, Iceberg, JDBC, etc.).' },
      { name: 'DATABASE', desc: 'Create a database namespace within the active catalog.' }
    ],
    returns: 'N/A (DDL)',
    example: "-- Source table backed by Kafka\nCREATE TABLE clicks (\n    user_id STRING,\n    url STRING,\n    ts TIMESTAMP(3),\n    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n    'connector' = 'kafka',\n    'topic' = 'web-clicks',\n    'properties.bootstrap.servers' = 'kafka:9092',\n    'format' = 'json'\n);",
    pipeline: "CREATE TABLE with a connector registers the logical schema; data flows only when an INSERT INTO (or SELECT) executes. Always define WATERMARK on event-time columns for windowed queries."
  },
  'ROUND': {
    signature: 'ROUND(numeric, scale)',
    category: 'Math',
    description: "Rounds a numeric value to the specified number of decimal places. Uses banker's rounding (HALF_EVEN) by default in Flink SQL.",
    params: [
      { name: 'numeric', desc: 'Numeric column or expression to round.' },
      { name: 'scale',   desc: 'Number of decimal places. Use 0 for integer rounding.' }
    ],
    returns: 'Same type as numeric',
    example: "SELECT order_id,\n       ROUND(amount, 2) AS rounded_amount,\n       ROUND(amount, 0) AS whole_number\nFROM orders;",
    pipeline: "Use ROUND before writing to JDBC sinks with fixed-precision DECIMAL columns to avoid precision mismatch errors."
  },
  'TABLE': {
    signature: 'TABLE(tvf_name(TABLE source, DESCRIPTOR(col), INTERVAL ...))',
    category: 'Table Valued Function',
    description: "Invokes a Table Valued Function (TVF) that returns a relation. In Flink SQL, TABLE() is used to call windowing TVFs (TUMBLE, HOP, SESSION, CUMULATE) and other polymorphic table functions. Returns rows with additional window metadata columns.",
    params: [
      { name: 'tvf_name',    desc: 'The TVF to call: TUMBLE, HOP, SESSION, CUMULATE.' },
      { name: 'TABLE source',desc: 'The input table or sub-query.' },
      { name: 'DESCRIPTOR',  desc: 'Specifies which column is the time attribute.' },
      { name: 'INTERVAL',    desc: 'Window size (and optionally slide interval for HOP).' }
    ],
    returns: 'Table with added window_start, window_end, window_time columns',
    example: "-- Tumble window using TVF syntax (Flink 1.13+)\nSELECT window_start, window_end, COUNT(*) AS cnt\nFROM TABLE(\n    TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' MINUTE)\n)\nGROUP BY window_start, window_end;",
    pipeline: "The TVF syntax (using TABLE()) is the preferred windowing approach from Flink 1.13 onwards. It supports emitting windows in INSERT INTO mode and exposes window_time for downstream watermark propagation."
  },
  'CATALOG': {
    signature: 'CREATE CATALOG name WITH (...) | USE CATALOG name | SHOW CATALOGS',
    category: 'Catalog',
    description: "A catalog is the top-level namespace that contains databases and tables. Flink supports multiple catalogs simultaneously. The default catalog is 'default_catalog'. External catalogs (Hive, Iceberg, JDBC) persist metadata across sessions.",
    params: [
      { name: 'name',    desc: 'Catalog identifier.' },
      { name: 'WITH (...)', desc: "Catalog-specific properties, e.g. 'type' = 'hive', 'hive-conf-dir' = '/etc/hive/conf'." }
    ],
    returns: 'N/A (DDL / utility)',
    example: "-- Register Hive catalog\nCREATE CATALOG hive_prod WITH (\n    'type' = 'hive',\n    'hive-conf-dir' = '/etc/hive/conf',\n    'default-database' = 'analytics'\n);\nUSE CATALOG hive_prod;\nSHOW DATABASES;",
    pipeline: "Register external catalogs at the start of long-running scripts so table names resolve across sessions. USE CATALOG must come before any USE DATABASE or table references in that catalog."
  },
  'DATABASE': {
    signature: 'CREATE DATABASE [IF NOT EXISTS] name | USE name | SHOW DATABASES',
    category: 'Catalog',
    description: "A database is a namespace within a catalog that groups related tables. Use CREATE DATABASE to organise tables by domain. USE switches the active database so table names can be referenced without a catalog prefix.",
    params: [
      { name: 'name', desc: 'Database identifier. Scoped within the active catalog.' }
    ],
    returns: 'N/A (DDL / utility)',
    example: "CREATE DATABASE IF NOT EXISTS fraud_detection;\nUSE fraud_detection;\n-- Tables are now created in fraud_detection\nCREATE TABLE events ( ... ) WITH ( ... );",
    pipeline: "Always explicitly USE DATABASE in production scripts to avoid accidentally creating tables in 'default'. Each Flink session starts in 'default_catalog.default' unless changed."
  },
  'SHOW TABLES': {
    signature: 'SHOW TABLES [FROM | IN catalog.database] [LIKE pattern]',
    category: 'Utility',
    description: "Lists all tables (and views) in the current or specified database. Supports optional pattern matching with LIKE.",
    params: [
      { name: 'FROM / IN', desc: '(Optional) Fully qualified database path.' },
      { name: 'LIKE pattern', desc: "(Optional) SQL pattern with % wildcard, e.g. LIKE 'kafka_%'." }
    ],
    returns: 'List of table names',
    example: "SHOW TABLES;\nSHOW TABLES FROM hive_prod.analytics;\nSHOW TABLES LIKE 'kafka_%';",
    pipeline: "Use at session start to verify available sources and sinks. Combine with DESCRIBE to inspect schemas before building queries."
  },
  'SHOW CATALOGS': {
    signature: 'SHOW CATALOGS',
    category: 'Utility',
    description: "Lists all catalogs registered in the current Flink SQL session.",
    params: [],
    returns: 'List of catalog names',
    example: "SHOW CATALOGS;",
    pipeline: "Use after connecting to verify which external catalogs are registered. If a catalog is missing, register it with CREATE CATALOG."
  },
  'SHOW DATABASES': {
    signature: 'SHOW DATABASES',
    category: 'Utility',
    description: "Lists all databases in the current catalog.",
    params: [],
    returns: 'List of database names',
    example: "SHOW DATABASES;",
    pipeline: "Run after USE CATALOG to see which databases are available. Pipe output into USE to switch context."
  },
  'SHOW FUNCTIONS': {
    signature: 'SHOW [USER] FUNCTIONS',
    category: 'Utility',
    description: "Lists all functions available in the current session. USER FUNCTIONS restricts output to user-defined functions (not built-ins).",
    params: [{ name: 'USER', desc: '(Optional) Show only UDFs, exclude built-in functions.' }],
    returns: 'List of function names',
    example: "SHOW FUNCTIONS;\nSHOW USER FUNCTIONS;",
    pipeline: "Use after ADD JAR and CREATE FUNCTION to confirm the UDF registered correctly and its name is callable."
  },
  'DESCRIBE': {
    signature: 'DESCRIBE | DESC table_name',
    category: 'Utility',
    description: "Shows the schema of a table or view: column names, data types, nullability, computed columns, watermarks, and primary key constraints.",
    params: [{ name: 'table_name', desc: 'Table or view name (can be fully qualified: catalog.db.table).' }],
    returns: 'Schema metadata result set',
    example: "DESCRIBE orders;\nDESC hive_prod.analytics.events;",
    pipeline: "Always DESCRIBE source tables before writing aggregation or JOIN queries — type mismatches are the most common cause of planning failures in Flink SQL."
  },
  'EXPLAIN': {
    signature: 'EXPLAIN [PLAN FOR | CHANGELOG MODE FOR | JSON_EXECUTION_PLAN FOR] statement',
    category: 'Utility',
    description: "Returns the logical or physical execution plan Flink will produce for a query without actually running it. Use PLAN FOR for the default text plan, JSON_EXECUTION_PLAN for a machine-readable format.",
    params: [
      { name: 'PLAN FOR',              desc: '(Default) Human-readable execution plan.' },
      { name: 'CHANGELOG MODE FOR',    desc: 'Shows the changelog mode (append/retract/upsert) per operator.' },
      { name: 'JSON_EXECUTION_PLAN FOR',desc: 'Machine-readable JSON plan for tooling.' }
    ],
    returns: 'Execution plan string',
    example: "EXPLAIN\nSELECT user_id, COUNT(*)\nFROM events\nWHERE type = 'click'\nGROUP BY user_id, TUMBLE(ts, INTERVAL '1' MINUTE);\n\nEXPLAIN CHANGELOG MODE FOR\nSELECT user_id, SUM(amount) FROM orders GROUP BY user_id;",
    pipeline: "Run EXPLAIN before submitting long-running INSERT INTO jobs. Check for: unexpected cross-joins, missing watermarks, retraction mode on aggregations (which creates larger state), and operators with high parallelism mismatch."
  },

  'WITH_CONNECTOR': {
    signature: "WITH (\n    'connector' = '...',\n    'key' = 'value'\n)",
    category: 'Connector Config',
    description: "The WITH clause in CREATE TABLE defines how Flink connects to an external system. The 'connector' key is mandatory and determines which connector plugin handles I/O.",
    params: [
      { name: "'connector'", desc: "Required. E.g. 'kafka','jdbc','datagen','elasticsearch-7','upsert-kafka','blackhole','print','filesystem','pulsar','kinesis'." },
      { name: "'format'",    desc: "Serialisation format: 'json','avro','avro-confluent','csv','parquet','orc','raw'." },
      { name: 'other props', desc: 'Additional connector-specific properties. See Flink docs.' }
    ],
    returns: 'N/A (CREATE TABLE clause)',
    example: "CREATE TABLE src (\n    id STRING,\n    ts TIMESTAMP(3),\n    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n    'connector' = 'kafka',\n    'topic' = 'events',\n    'properties.bootstrap.servers' = 'kafka:9092',\n    'format' = 'json'\n);",
    pipeline: 'All property keys must be lowercase string literals in single quotes. Connector JARs must be on the Flink classpath. Test connectivity before submitting jobs.'
  }
};


// ── AUTOCOMPLETE STATE ──────────────────────────
let _acItems = [];
let _acIdx = -1;
let _acActive = false;
let _acTimer = null;

// ── THEME DETECTION HELPERS ─────────────────────
function _isDarkTheme() {
  // Check common dark-mode signals: body/html class, data-theme attribute, prefers-color-scheme
  const root = document.documentElement;
  const body = document.body;
  if (root.classList.contains('dark') || body.classList.contains('dark')) return true;
  if (root.getAttribute('data-theme') === 'dark' || body.getAttribute('data-theme') === 'dark') return true;
  if (root.classList.contains('theme-dark') || body.classList.contains('theme-dark')) return true;
  // Check computed background brightness of the editor or body
  const editorEl = document.getElementById('sql-editor');
  const target = editorEl || body;
  const bg = window.getComputedStyle(target).backgroundColor;
  const rgb = bg.match(/\d+/g);
  if (rgb) {
    const brightness = (parseInt(rgb[0]) * 299 + parseInt(rgb[1]) * 587 + parseInt(rgb[2]) * 114) / 1000;
    return brightness < 128;
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

function _getThemeColors() {
  const dark = _isDarkTheme();
  return {
    // Autocomplete dropdown
    acBg:           dark ? '#1e2130' : '#ffffff',
    acBorder:       dark ? '#3a3f55' : '#d0d5dd',
    acText:         dark ? '#e2e8f0' : '#1a202c',
    acHintBg:       dark ? '#161825' : '#f8f9fa',
    acHintText:     dark ? '#6b7280' : '#9ca3af',
    acItemBorder:   dark ? '#2d3148' : '#f0f2f5',
    acHoverBg:      dark ? '#2a3050' : '#eff6ff',
    acHoverText:    dark ? '#93c5fd' : '#1e40af',
    acSelectedBg:   dark ? '#1d4ed8' : '#2563eb',
    acSelectedText: dark ? '#ffffff' : '#ffffff',
    acDetailText:   dark ? '#6b7280' : '#9ca3af',
    // Badges — same in both themes (high contrast coloured pills)
    badgeKwBg:      dark ? '#1e3a5f' : '#dbeafe',
    badgeKwText:    dark ? '#93c5fd' : '#1e3a5f',
    badgeFnBg:      dark ? '#14532d' : '#dcfce7',
    badgeFnText:    dark ? '#86efac' : '#14532d',
    badgeTpBg:      dark ? '#713f12' : '#fef3c7',
    badgeTpText:    dark ? '#fcd34d' : '#92400e',
    badgeSnipBg:    dark ? '#3b0764' : '#ede9fe',
    badgeSnipText:  dark ? '#c4b5fd' : '#5b21b6',
    badgeTblBg:     dark ? '#500724' : '#fce7f3',
    badgeTblText:   dark ? '#f9a8d4' : '#9d174d',
    // Tooltip
    ttBg:           dark ? '#1a1d2e' : '#ffffff',
    ttBorder:       dark ? '#3a3f55' : '#d0d5dd',
    ttText:         dark ? '#e2e8f0' : '#1a202c',
    ttMuted:        dark ? '#94a3b8' : '#64748b',
    ttCatBg:        dark ? '#1e3a5f' : '#eff6ff',
    ttCatText:      dark ? '#93c5fd' : '#1e40af',
    ttCodeBg:       dark ? '#0f1117' : '#f1f5f9',
    ttCodeText:     dark ? '#a5f3fc' : '#0f4c75',
    ttLabelText:    dark ? '#64748b' : '#94a3b8',
    ttSectionBg:    dark ? '#161825' : '#f8fafc',
    ttSectionBorder:dark ? '#2d3148' : '#e2e8f0',
    ttScrollBar:    dark ? '#3a3f55' : '#d1d5db',
  };
}

// Inject the autocomplete dropdown into the DOM once
(function injectACBox() {
  if (document.getElementById('autocomplete-box')) return;
  const box = document.createElement('div');
  box.id = 'autocomplete-box';
  box.innerHTML = '<div id="ac-list"></div><div id="ac-hint">Tab or Enter to insert &nbsp;·&nbsp; ↑↓ navigate &nbsp;·&nbsp; Esc close</div>';
  document.body.appendChild(box);

  const style = document.createElement('style');
  style.id = 'ac-styles';
  style.textContent = _buildACStyles();
  document.head.appendChild(style);

  // Re-apply styles when OS/user theme changes
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', _refreshStyles);
})();

function _buildACStyles() {
  const c = _getThemeColors();
  return `
    #autocomplete-box {
      position: fixed;
      z-index: 99999;
      background: ${c.acBg};
      border: 1px solid ${c.acBorder};
      border-radius: 7px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.18);
      min-width: 260px;
      max-width: 400px;
      max-height: 260px;
      overflow-y: auto;
      display: none;
      font-family: monospace;
      font-size: 12px;
      color: ${c.acText};
      scrollbar-width: thin;
      scrollbar-color: ${c.acScrollBar || c.acBorder} transparent;
    }
    #autocomplete-box::-webkit-scrollbar { width: 5px; }
    #autocomplete-box::-webkit-scrollbar-track { background: transparent; }
    #autocomplete-box::-webkit-scrollbar-thumb { background: ${c.acBorder}; border-radius: 3px; }
    #ac-hint {
      font-family: sans-serif;
      font-size: 10px;
      padding: 4px 10px;
      background: ${c.acHintBg};
      border-top: 1px solid ${c.acItemBorder};
      color: ${c.acHintText};
      letter-spacing: 0.02em;
    }
    .ac-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      cursor: pointer;
      border-bottom: 1px solid ${c.acItemBorder};
      transition: background 0.1s;
    }
    .ac-item:last-child { border-bottom: none; }
    .ac-item:hover { background: ${c.acHoverBg}; color: ${c.acHoverText}; }
    .ac-item.ac-selected { background: ${c.acSelectedBg}; color: ${c.acSelectedText}; }
    .ac-item.ac-selected .ac-detail { color: rgba(255,255,255,0.65); }
    .ac-badge {
      font-size: 9px;
      padding: 2px 5px;
      border-radius: 3px;
      font-family: sans-serif;
      font-weight: 700;
      flex-shrink: 0;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .ac-badge-kw   { background: ${c.badgeKwBg};   color: ${c.badgeKwText}; }
    .ac-badge-fn   { background: ${c.badgeFnBg};   color: ${c.badgeFnText}; }
    .ac-badge-tp   { background: ${c.badgeTpBg};   color: ${c.badgeTpText}; }
    .ac-badge-snip { background: ${c.badgeSnipBg}; color: ${c.badgeSnipText}; }
    .ac-badge-tbl  { background: ${c.badgeTblBg};  color: ${c.badgeTblText}; }
    .ac-label { flex: 1; color: inherit; }
    .ac-detail { font-size: 10px; color: ${c.acDetailText}; font-family: sans-serif; }

    /* ── Hover Tooltip (JavaDoc-style) ── */
    #sql-doc-tooltip {
      position: fixed;
      z-index: 99998;
      background: ${c.ttBg};
      border: 1px solid ${c.ttBorder};
      border-radius: 8px;
      box-shadow: 0 12px 32px rgba(0,0,0,0.22);
      width: 420px;
      max-height: 420px;
      overflow-y: auto;
      display: none;
      font-family: sans-serif;
      font-size: 12px;
      color: ${c.ttText};
      scrollbar-width: thin;
      scrollbar-color: ${c.ttBorder} transparent;
    }
    #sql-doc-tooltip::-webkit-scrollbar { width: 5px; }
    #sql-doc-tooltip::-webkit-scrollbar-track { background: transparent; }
    #sql-doc-tooltip::-webkit-scrollbar-thumb { background: ${c.ttBorder}; border-radius: 3px; }
    .tt-header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
      padding: 12px 14px 10px;
      border-bottom: 1px solid ${c.ttSectionBorder};
    }
    .tt-sig {
      font-family: monospace;
      font-size: 12px;
      font-weight: 700;
      color: ${c.ttCodeText};
      word-break: break-all;
      flex: 1;
    }
    .tt-cat {
      font-size: 10px;
      padding: 2px 7px;
      border-radius: 4px;
      background: ${c.ttCatBg};
      color: ${c.ttCatText};
      font-weight: 600;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .tt-close {
      cursor: pointer;
      font-size: 15px;
      color: ${c.ttMuted};
      flex-shrink: 0;
      line-height: 1;
      padding: 0 0 0 4px;
    }
    .tt-close:hover { color: ${c.ttText}; }
    .tt-body { padding: 10px 14px; line-height: 1.55; }
    .tt-desc { margin-bottom: 10px; color: ${c.ttText}; }
    .tt-section {
      margin-bottom: 10px;
      background: ${c.ttSectionBg};
      border-left: 3px solid ${c.ttBorder};
      border-radius: 0 5px 5px 0;
      padding: 8px 10px;
    }
    .tt-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: ${c.ttLabelText};
      margin-bottom: 4px;
    }
    .tt-params { margin: 0; padding: 0; list-style: none; }
    .tt-params li { margin-bottom: 4px; }
    .tt-param-name {
      font-family: monospace;
      font-size: 11px;
      color: ${c.ttCodeText};
      background: ${c.ttCodeBg};
      padding: 1px 5px;
      border-radius: 3px;
      margin-right: 4px;
    }
    .tt-param-desc { color: ${c.ttText}; }
    .tt-returns {
      font-family: monospace;
      font-size: 11px;
      color: ${c.ttCodeText};
      background: ${c.ttCodeBg};
      padding: 2px 7px;
      border-radius: 3px;
      display: inline-block;
    }
    .tt-code {
      font-family: monospace;
      font-size: 11px;
      background: ${c.ttCodeBg};
      color: ${c.ttCodeText};
      padding: 8px 10px;
      border-radius: 5px;
      white-space: pre;
      overflow-x: auto;
      line-height: 1.5;
      margin: 0;
    }
    .tt-pipeline-text { color: ${c.ttText}; font-size: 12px; line-height: 1.55; }
  `;
}

function _refreshStyles() {
  const style = document.getElementById('ac-styles');
  if (style) style.textContent = _buildACStyles();
  // Also refresh tooltip if visible
  const tt = document.getElementById('sql-doc-tooltip');
  if (tt && tt.style.display !== 'none') _hideTooltip();
}

// ── TOOLTIP DOM INJECTION ───────────────────────
(function injectTooltip() {
  if (document.getElementById('sql-doc-tooltip')) return;
  const tt = document.createElement('div');
  tt.id = 'sql-doc-tooltip';
  document.body.appendChild(tt);
})();

function _showTooltip(keyword, anchorRect) {
  const doc = DOCS[keyword.toUpperCase()];
  if (!doc) return;

  const tt = document.getElementById('sql-doc-tooltip');
  if (!tt) return;

  const paramsHtml = doc.params && doc.params.length
      ? `<div class="tt-section">
        <div class="tt-label">Parameters</div>
        <ul class="tt-params">${doc.params.map(p =>
          `<li><span class="tt-param-name">${p.name}</span><span class="tt-param-desc">${p.desc}</span></li>`
      ).join('')}</ul>
       </div>`
      : '';

  const returnsHtml = doc.returns
      ? `<div class="tt-section">
        <div class="tt-label">Returns</div>
        <span class="tt-returns">${doc.returns}</span>
       </div>`
      : '';

  const exampleHtml = doc.example
      ? `<div class="tt-section">
        <div class="tt-label">Example</div>
        <pre class="tt-code">${_escapeHtml(doc.example)}</pre>
       </div>`
      : '';

  const pipelineHtml = doc.pipeline
      ? `<div class="tt-section">
        <div class="tt-label">Pipeline usage</div>
        <p class="tt-pipeline-text">${doc.pipeline}</p>
       </div>`
      : '';

  tt.innerHTML = `
    <div class="tt-header">
      <span class="tt-sig">${_escapeHtml(doc.signature)}</span>
      <span class="tt-cat">${doc.category}</span>
      <span class="tt-close" id="tt-close-btn" title="Close">&#x2715;</span>
    </div>
    <div class="tt-body">
      <p class="tt-desc">${doc.description}</p>
      ${paramsHtml}
      ${returnsHtml}
      ${exampleHtml}
      ${pipelineHtml}
    </div>`;

  document.getElementById('tt-close-btn').addEventListener('click', _hideTooltip);

  // Position the tooltip smartly around the anchor word
  tt.style.display = 'block';
  const ttW = 420, ttH = Math.min(420, tt.scrollHeight);
  const vw = window.innerWidth, vh = window.innerHeight;
  let top = anchorRect.bottom + 6;
  let left = anchorRect.left;
  if (top + ttH > vh - 10) top = anchorRect.top - ttH - 6;
  if (left + ttW > vw - 10) left = vw - ttW - 12;
  if (left < 6) left = 6;
  if (top < 6) top = 6;
  tt.style.top  = top + 'px';
  tt.style.left = left + 'px';
}

function _hideTooltip() {
  const tt = document.getElementById('sql-doc-tooltip');
  if (tt) tt.style.display = 'none';
}

function _escapeHtml(str) {
  return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── WORD UNDER CURSOR DETECTION ─────────────────
function _getWordAtPos(text, pos) {
  let start = pos, end = pos;
  while (start > 0 && /[\w]/.test(text[start - 1])) start--;
  while (end < text.length && /[\w]/.test(text[end])) end++;
  const single = text.slice(start, end).toUpperCase();

  // Try two-word combos e.g. "GROUP BY", "ORDER BY", "PARTITION BY", etc.
  const afterEnd = text.slice(end).match(/^\s+(\w+)/);
  if (afterEnd) {
    const twoWord = (single + ' ' + afterEnd[1]).toUpperCase();
    if (DOCS[twoWord]) return twoWord;
  }
  const beforeStart = text.slice(0, start).match(/(\w+)\s+$/);
  if (beforeStart) {
    const twoWord = (beforeStart[1] + ' ' + single).toUpperCase();
    if (DOCS[twoWord]) return twoWord;
  }

  // Special case: hovering 'WITH' near connector property → show connector docs
  if (single === 'WITH') {
    const nearby = text.slice(Math.max(0, pos - 5), pos + 80).toUpperCase();
    if (nearby.includes("'CONNECTOR'") || nearby.includes('CONNECTOR')) {
      return 'WITH (connector)';
    }
  }

  return single;
}

// ── HOVER TOOLTIP TRIGGER ───────────────────────
let _hoverTimer = null;

document.addEventListener('DOMContentLoaded', function () {
  const ed = document.getElementById('sql-editor');
  if (!ed) return;

  // Hover: show tooltip after 600ms pause over a known keyword
  ed.addEventListener('mousemove', function (e) {
    clearTimeout(_hoverTimer);
    _hoverTimer = setTimeout(() => {
      // Estimate character position from mouse coords
      const rect = ed.getBoundingClientRect();
      const style = window.getComputedStyle(ed);
      const lineH = parseFloat(style.lineHeight) || 20;
      const charW = 7.8;
      const relY = e.clientY - rect.top + ed.scrollTop;
      const relX = e.clientX - rect.left;
      const lineNum = Math.floor(relY / lineH);
      const colNum  = Math.floor((relX - 14) / charW);
      const lines   = ed.value.split('\n');
      if (lineNum < 0 || lineNum >= lines.length) return;
      const lineText = lines[lineNum];
      const approxPos = Math.max(0, Math.min(colNum, lineText.length));
      const word = _getWordAtPos(lineText, approxPos);
      if (word && DOCS[word]) {
        const anchorRect = { top: e.clientY - 10, bottom: e.clientY + 10, left: e.clientX };
        _showTooltip(word, anchorRect);
      } else {
        _hideTooltip();
      }
    }, 600);
  });

  ed.addEventListener('mouseleave', function () {
    clearTimeout(_hoverTimer);
    // Give user time to move mouse into the tooltip itself
    setTimeout(() => {
      const tt = document.getElementById('sql-doc-tooltip');
      if (tt && !tt.matches(':hover')) _hideTooltip();
    }, 300);
  });

  // Click: show tooltip immediately on known keyword under caret
  ed.addEventListener('click', function () {
    const pos  = ed.selectionStart;
    const text = ed.value;
    const word = _getWordAtPos(text, pos);
    if (word && DOCS[word]) {
      // Build an anchor rect from the caret position
      const rect   = ed.getBoundingClientRect();
      const txt    = text.slice(0, pos);
      const lines  = txt.split('\n');
      const lineH  = parseFloat(window.getComputedStyle(ed).lineHeight) || 20;
      const charW  = 7.8;
      const lineIdx = lines.length - 1;
      const col     = lines[lineIdx].length;
      const top  = rect.top  + lineIdx * lineH - ed.scrollTop + lineH;
      const left = rect.left + col * charW + 14;
      _showTooltip(word, { top, bottom: top + lineH, left });
    } else {
      _hideTooltip();
    }
    updateCursorPos();
    _hideAC();
  });
});

// Keep tooltip open when mouse is inside it; close when leaving
document.addEventListener('mouseleave', function (e) {
  if (e.target && e.target.id === 'sql-doc-tooltip') _hideTooltip();
}, true);

// Close tooltip on outside click
document.addEventListener('click', function (e) {
  const tt = document.getElementById('sql-doc-tooltip');
  const ed = document.getElementById('sql-editor');
  if (tt && !tt.contains(e.target) && e.target !== ed) _hideTooltip();
}, true);

function _buildACItems(word) {
  const w = word.toLowerCase();
  if (!w || w.length < 1) return [];
  const results = [];

  // Snippets first
  AC_SNIPPETS
      .filter(s => s.label.toLowerCase().startsWith(w))
      .forEach(s => results.push({ label: s.label, insert: s.insert, badge: 'snip', detail: s.detail }));

  // Keywords
  AC_KEYWORDS
      .filter(k => k.toLowerCase().startsWith(w) && k.toLowerCase() !== w)
      .forEach(k => results.push({ label: k, insert: k + ' ', badge: 'kw', detail: 'keyword' }));

  // Functions
  AC_FUNCTIONS
      .filter(f => f.toLowerCase().startsWith(w))
      .forEach(f => results.push({ label: f, insert: f, badge: 'fn', detail: 'function' }));

  // Types
  AC_TYPES
      .filter(t => t.toLowerCase().startsWith(w))
      .forEach(t => results.push({ label: t, insert: t, badge: 'tp', detail: 'type' }));

  // Session tables (pull from your existing state if available)
  const tables = (typeof state !== 'undefined' && state.sessionTables) ? state.sessionTables : [];
  tables
      .filter(t => t.toLowerCase().startsWith(w) && t.toLowerCase() !== w)
      .forEach(t => results.push({ label: t, insert: t + ' ', badge: 'tbl', detail: 'table' }));

  return results.slice(0, 14);
}

function _getWordBefore(pos) {
  const txt = document.getElementById('sql-editor').value.slice(0, pos);
  const m = txt.match(/[\w.*<]+$/);
  return m ? m[0] : '';
}

function _showAC() {
  const ed = document.getElementById('sql-editor');
  const word = _getWordBefore(ed.selectionStart);
  _acItems = _buildACItems(word);
  if (!_acItems.length) { _hideAC(); return; }

  const acList = document.getElementById('ac-list');
  acList.innerHTML = _acItems.map((item, i) => `
    <div class="ac-item${i === _acIdx ? ' ac-selected' : ''}" data-idx="${i}">
      <span class="ac-badge ac-badge-${item.badge}">${item.badge}</span>
      <span class="ac-label">${item.label}</span>
      <span class="ac-detail">${item.detail}</span>
    </div>`).join('');

  acList.querySelectorAll('.ac-item').forEach(el => {
    el.addEventListener('mousedown', e => {
      e.preventDefault();
      _applyAC(parseInt(el.dataset.idx));
    });
  });

  document.getElementById('autocomplete-box').style.display = 'block';
  _acActive = true;
  _positionAC();
}

function _positionAC() {
  const ed = document.getElementById('sql-editor');
  const box = document.getElementById('autocomplete-box');
  const rect = ed.getBoundingClientRect();
  const pos = ed.selectionStart;
  const txt = ed.value.slice(0, pos);
  const lines = txt.split('\n');
  const lineNum = lines.length - 1;
  const col = lines[lineNum].length;
  const lineH = parseFloat(getComputedStyle(ed).lineHeight) || 20;
  const charW = 7.8; // approximate monospace char width at ~13px
  const top = rect.top + (lineNum + 1) * lineH - ed.scrollTop + 2;
  const left = rect.left + col * charW + 14;
  box.style.top  = Math.min(top, window.innerHeight - 260) + 'px';
  box.style.left = Math.min(left, window.innerWidth - 400) + 'px';
}

function _hideAC() {
  const box = document.getElementById('autocomplete-box');
  if (box) box.style.display = 'none';
  _acActive = false;
  _acIdx = -1;
}

function _selectAC(dir) {
  _acIdx = Math.max(0, Math.min(_acItems.length - 1, _acIdx + dir));
  document.querySelectorAll('#ac-list .ac-item').forEach((el, i) => {
    el.classList.toggle('ac-selected', i === _acIdx);
    if (i === _acIdx) el.scrollIntoView({ block: 'nearest' });
  });
}

function _applyAC(idx) {
  if (idx < 0) idx = Math.max(0, _acIdx);
  const item = _acItems[idx];
  if (!item) return;
  const ed = document.getElementById('sql-editor');
  const pos = ed.selectionStart;
  const word = _getWordBefore(pos);
  const before = ed.value.slice(0, pos - word.length);
  const after  = ed.value.slice(pos);
  ed.value = before + item.insert + after;
  const newPos = before.length + item.insert.length;
  ed.selectionStart = ed.selectionEnd = newPos;
  _hideAC();
  updateLineNumbers();
  updateCursorPos();
  ed.focus();
}

// ── SMART NEWLINE (IntelliSense indentation) ────
function _smartNewline(el) {
  const pos = el.selectionStart;
  const before = el.value.slice(0, pos);
  const after  = el.value.slice(pos);
  const lines = before.split('\n');
  const curLine = lines[lines.length - 1];
  const indentMatch = curLine.match(/^(\s*)/);
  let indent = indentMatch ? indentMatch[1] : '';

  // Add one extra indent level after clause starters
  const INDENT_AFTER = /^(SELECT|FROM|WHERE|WITH|JOIN|LEFT|RIGHT|INNER|FULL|CROSS|ON|GROUP\s+BY|HAVING|ORDER\s+BY|CASE|WHEN|THEN|ELSE)\b/i;
  if (INDENT_AFTER.test(curLine.trimStart())) indent += '    ';

  el.value = before + '\n' + indent + after;
  el.selectionStart = el.selectionEnd = pos + 1 + indent.length;
  updateLineNumbers();
  updateCursorPos();
}

// ── EDITOR EVENT LISTENERS ──────────────────────
let _persistTimer = null;
let _warnTimer = null;

document.getElementById('sql-editor').addEventListener('input', function () {
  // Autocomplete trigger
  clearTimeout(_acTimer);
  _acTimer = setTimeout(() => { _acIdx = -1; _showAC(); }, 120);

  // Save to active tab (debounced)
  clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    const cur = state.tabs.find(t => t.id === state.activeTab);
    if (cur) { cur.sql = this.value; persistWorkspace(); }
  }, 1500);

  // Inline Flink SQL sanity warnings (debounced)
  clearTimeout(_warnTimer);
  _warnTimer = setTimeout(() => {
    const val = this.value.toLowerCase();
    const warn = document.getElementById('editor-warn');
    if (!warn) return;
    if (val.includes('information_schema')) {
      warn.textContent = '⚠ Flink SQL does not support information_schema. Use SHOW TABLES; / DESCRIBE <table>; / SHOW CREATE TABLE <table>;';
      warn.style.display = 'block';
    } else if (/^select/m.test(val) && !val.includes('use catalog') && !state.activeSession) {
      warn.textContent = '⚠ No active session. Create a session first.';
      warn.style.display = 'block';
    } else {
      warn.style.display = 'none';
    }
  }, 600);

  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) { tab.sql = this.value; tab.saved = false; }
  updateLineNumbers();
  renderTabs();
});

document.getElementById('sql-editor').addEventListener('keydown', function (e) {
  // ── Autocomplete navigation
  if (_acActive) {
    if (e.key === 'ArrowDown') { e.preventDefault(); _selectAC(1); return; }
    if (e.key === 'ArrowUp')   { e.preventDefault(); _selectAC(-1); return; }
    if (e.key === 'Tab' || e.key === 'Enter') {
      if (_acItems.length) { e.preventDefault(); _applyAC(_acIdx >= 0 ? _acIdx : 0); return; }
    }
    if (e.key === 'Escape') { e.preventDefault(); _hideAC(); return; }
  }

  // Ctrl+Space → force open autocomplete
  if (e.ctrlKey && e.key === ' ') { e.preventDefault(); _acIdx = -1; _showAC(); return; }

  // Ctrl+Enter → run
  if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); executeSQL(); return; }
  // Ctrl+S → save
  if (e.ctrlKey && e.key === 's') { e.preventDefault(); saveQuery(); return; }
  // Ctrl+/ → comment
  if (e.ctrlKey && e.key === '/') { e.preventDefault(); toggleComment(this); return; }

  // Enter → smart newline with auto-indent
  if (e.key === 'Enter' && !_acActive) {
    e.preventDefault();
    _smartNewline(this);
    return;
  }

  // Tab → indent (with multi-line selection support)
  if (e.key === 'Tab' && !_acActive) {
    e.preventDefault();
    const s = this.selectionStart, end = this.selectionEnd;
    if (s === end) {
      // Single cursor: insert 4 spaces
      this.value = this.value.slice(0, s) + '    ' + this.value.slice(end);
      this.selectionStart = this.selectionEnd = s + 4;
    } else {
      // Multi-line selection: indent each line
      const lines = this.value.split('\n');
      let pos = 0, sl = 0, el = 0;
      for (let i = 0; i < lines.length; i++) {
        if (pos <= s && sl === 0) sl = i;
        if (pos <= end) el = i;
        pos += lines[i].length + 1;
      }
      for (let i = sl; i <= el; i++) lines[i] = '    ' + lines[i];
      this.value = lines.join('\n');
    }
    updateLineNumbers();
    return;
  }
});

// Note: editor click is handled inside the DOMContentLoaded block above
// (tooltip detection + _hideAC + updateCursorPos are all wired there)

document.getElementById('sql-editor').addEventListener('keyup', updateCursorPos);

// Outside-click for autocomplete is handled in the DOMContentLoaded block and the
// document click listener added alongside the tooltip logic above.

document.getElementById('sql-editor').addEventListener('scroll', function () {
  document.getElementById('line-numbers').scrollTop = this.scrollTop;
});

// ── CURSOR / LINE NUMBER HELPERS ────────────────
function updateCursorPos() {
  const ed = document.getElementById('sql-editor');
  const txt = ed.value.slice(0, ed.selectionStart);
  const lines = txt.split('\n');
  document.getElementById('cursor-pos').textContent = `Ln ${lines.length}, Col ${lines[lines.length - 1].length + 1}`;
}

function updateLineNumbers() {
  const ed = document.getElementById('sql-editor');
  const lines = ed.value.split('\n').length;
  const ln = document.getElementById('line-numbers');
  ln.innerHTML = Array.from({ length: lines }, (_, i) => `<span>${i + 1}</span>`).join('');
  ln.scrollTop = ed.scrollTop;
}

// ── EDITOR ACTIONS ──────────────────────────────
function toggleComment(el) {
  const start = el.selectionStart, end = el.selectionEnd;
  const lines = el.value.split('\n');
  let pos = 0, startLine = 0, endLine = 0;
  for (let i = 0; i < lines.length; i++) {
    if (pos + lines[i].length >= start && startLine === 0) startLine = i;
    if (pos + lines[i].length >= end) { endLine = i; break; }
    pos += lines[i].length + 1;
  }
  const allCommented = lines.slice(startLine, endLine + 1).every(l => l.trimStart().startsWith('--'));
  for (let i = startLine; i <= endLine; i++) {
    lines[i] = allCommented ? lines[i].replace(/^(\s*)--\s?/, '$1') : '-- ' + lines[i];
  }
  el.value = lines.join('\n');
  updateLineNumbers();
}

function clearEditor() {
  document.getElementById('sql-editor').value = '';
  updateLineNumbers();
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) { tab.sql = ''; tab.saved = false; }
  renderTabs();
}

function saveQuery() {
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) {
    tab.sql = document.getElementById('sql-editor').value;
    tab.saved = true;
    renderTabs();
    toast('Query saved', 'ok');
  }
}

function formatSQL() {
  const ed = document.getElementById('sql-editor');
  let sql = ed.value.trim();
  if (!sql) { toast('Editor is empty', 'err'); return; }

  // ── Step 1: Normalize whitespace
  sql = sql.replace(/\r\n/g, '\n').replace(/\t/g, '    ');

  // ── Step 2: Uppercase keywords
  const KW = [
    'SELECT','DISTINCT','FROM','WHERE','GROUP\\s+BY','HAVING','ORDER\\s+BY','LIMIT',
    'INSERT\\s+INTO','VALUES','UPDATE','SET','DELETE\\s+FROM','DELETE',
    'CREATE\\s+TEMPORARY\\s+TABLE','CREATE\\s+TABLE','CREATE\\s+VIEW',
    'CREATE\\s+DATABASE','CREATE\\s+CATALOG','DROP\\s+TABLE','DROP\\s+VIEW',
    'ALTER\\s+TABLE','DESCRIBE','EXPLAIN','SHOW\\s+TABLES','SHOW\\s+DATABASES',
    'SHOW\\s+CATALOGS','SHOW\\s+FUNCTIONS','SHOW\\s+JOBS','USE\\s+CATALOG','USE',
    'UNION\\s+ALL','UNION','INTERSECT','EXCEPT',
    'LEFT\\s+OUTER\\s+JOIN','RIGHT\\s+OUTER\\s+JOIN','FULL\\s+OUTER\\s+JOIN',
    'LEFT\\s+JOIN','RIGHT\\s+JOIN','INNER\\s+JOIN','CROSS\\s+JOIN','JOIN',
    'ON','AS','WITH','AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','IS\\s+NULL',
    'IS\\s+NOT\\s+NULL','CASE','WHEN','THEN','ELSE','END',
    'PARTITION\\s+BY','OVER','WINDOW','ROW_NUMBER','RANK','DENSE_RANK',
    'TUMBLE','HOP','SESSION','CUMULATE','DESCRIPTOR','WATERMARK\\s+FOR',
    'WATERMARK','INTERVAL','TIMESTAMP','BIGINT','STRING','INT','DOUBLE','BOOLEAN',
    'ARRAY','MAP','ROW','PRIMARY\\s+KEY','NOT\\s+ENFORCED','WITH',
    'ENFORCE','PROC_TIME\\(\\)','CURRENT_TIMESTAMP'
  ];
  KW.forEach(kw => {
    sql = sql.replace(new RegExp('\\b(' + kw + ')\\b', 'gi'), m => m.toUpperCase());
  });

  // ── Step 3: Line breaks before major clauses
  const TOP_LEVEL_CLAUSES = [
    'FROM','WHERE','GROUP BY','HAVING','ORDER BY','LIMIT',
    'LEFT JOIN','RIGHT JOIN','INNER JOIN','CROSS JOIN',
    'FULL OUTER JOIN','LEFT OUTER JOIN','RIGHT OUTER JOIN','JOIN',
    'UNION ALL','UNION','INTERSECT','EXCEPT',
    'INSERT INTO','VALUES','ON'
  ];
  TOP_LEVEL_CLAUSES.forEach(kw => {
    const re = new RegExp('\\s+(' + kw.replace(/ /g, '\\s+') + ')\\s+', 'g');
    sql = sql.replace(re, '\n' + kw + ' ');
  });

  // ── Step 4: Indent continuation lines with depth tracking
  const lines = sql.split('\n');
  const TOP_RE = /^(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|INSERT INTO|VALUES|CREATE|ALTER|DROP|SHOW|USE|DESCRIBE|EXPLAIN|WITH|UNION|INTERSECT|EXCEPT|JOIN|LEFT|RIGHT|INNER|CROSS|FULL|ON|CASE|WHEN|THEN|ELSE|END)\b/i;
  const result = [];
  let depth = 0;
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    const opens  = (line.match(/\(/g) || []).length;
    const closes = (line.match(/\)/g) || []).length;
    if (closes > opens) depth = Math.max(0, depth - (closes - opens));
    const isTopLevel = TOP_RE.test(line) && depth === 0;
    const indent = isTopLevel ? '' : '    '.repeat(Math.max(1, depth));
    result.push(indent + line);
    if (opens > closes) depth += (opens - closes);
  }
  sql = result.join('\n');

  ed.value = sql;
  updateLineNumbers();
  toast('SQL formatted', 'ok');
}

// ── SQL EXECUTION
// ─────────────────────────────────────