# Improvements

## Goroutine parallelism

### caveat
  1. For each configured Oracle database, start a goroutine.
  2. Inside each database scrape, for each configured metric/query, start another goroutine.
  3. Those Oracle queries run in parallel.
  4. When they finish, the collected rows are gathered together.
  5. Then PostgreSQL gets one bulk write for the whole pass.

  So if you have:

  - 30 Oracle databases
  - 20 metric queries per database

  the current shape can theoretically run up to around 30 * 20 = 600 Oracle metric query goroutines during a scrape pass, although actual database concurrency is also constrained by each Oracle database connection pool settings like maxOpenConns.

### improvement
Sequential queries per oracle DB
