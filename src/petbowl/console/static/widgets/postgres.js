import { esc, kv } from './generic.js';

export default {
  summary: s => [s.health === 'ok' ? `${s.rows} rows` : 'down', s.health === 'ok' && s.seconds_since_last_insert != null ? `last insert ${s.seconds_since_last_insert.toFixed(0)}s ago` : (s.error || '')],
  mount(el) { el.innerHTML = '<div id="c"></div><h3>Recent inserts</h3><div id="t"></div>'; },
  update(el, node) {
    const s = node.snapshot;
    el.querySelector('#c').innerHTML = kv({ health: s.health, rows: s.rows, ...Object.fromEntries(Object.entries(s.by_kind).map(([k, v]) => [`kind:${k}`, v])), ...(s.error ? { error: s.error } : {}) });
    el.querySelector('#t').innerHTML = `<table><tr><th>device</th><th>seq</th><th>kind</th><th>value</th></tr>${s.recent.map(r =>
      `<tr><td>${esc(r.device_id)}</td><td>${r.seq}</td><td>${esc(r.kind)}</td><td>${r.value ?? ''}</td></tr>`).join('')}</table>`;
  },
  onEvent() {},
};
