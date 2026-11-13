HTTP request (NDJSON body, N lines)
      |
      v
   app (3.4)   splits lines, calls process_batch(lines, PgEvents(pool), kv)
      |
      v
  service.process_batch
      |  1. parse each line -> Event, or REJECTED(reason)
      |
      |  2. events.insert(valid events) ------------> PgEvents.insert
      |                                                 |  pool gives a connection
      |                                                 |  BEGIN
      |                                                 |  INSERT ... unnest(...) ON CONFLICT DO NOTHING
      |                                                 |  Postgres returns only the rows it inserted
      |                                                 |  COMMIT (Postgres fsyncs its own WAL)
      |  <-- [True, False, True, ...] ----------------- |  insert_events turns that into one bool per event
      |
      |  3. per event: state.get(device) -> classify (gap / late / ok), advance in memory
      |
      |  4. per changed device: state.put(device, new state) ----> KV: append to WAL, then dict
      v
  BatchResult (outcome + detail per line) -> app turns it into the JSON response