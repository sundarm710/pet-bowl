import { esc, kv } from './generic.js';

export default {
  summary: s => [`${s.health} · ${s.rate_per_s}/s`, `acc ${s.counters.accepted} dup ${s.counters.duplicates} rej ${s.counters.rejected}`],
  mount(el) { el.innerHTML = '<div id="c"></div><h3>Reject reasons</h3><div id="r"></div>'; },
  update(el, node) {
    const s = node.snapshot, c = s.counters;
    el.querySelector('#c').innerHTML = kv({
      health: s.health, 'items/s': s.rate_per_s, batches: c.batches, items: c.items,
      accepted: c.accepted, duplicates: c.duplicates, rejected: c.rejected, gaps: c.gaps, late: c.late,
      'malformed seen at tap': c.malformed_seen, '4xx': c.http_4xx, '5xx': c.http_5xx,
      'upstream errors': c.upstream_errors, 'acked via ack-log': s.via_logs.acked, 'dead-lettered via log': s.via_logs.dead_lettered, 'last latency': `${s.last_latency_ms} ms`,
    });
    const rr = Object.entries(s.reject_reasons);
    el.querySelector('#r').innerHTML = rr.length ? `<table>${rr.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${v}</td></tr>`).join('')}</table>` : '<span class="muted">none</span>';
  },
  onEvent() {},
};
