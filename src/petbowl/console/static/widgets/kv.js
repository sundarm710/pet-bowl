import { esc, kv } from './generic.js';

export default {
  summary: s => [`${s.health} · ${s.devices_with_state} devices`, s.behind ? `${s.behind} behind` : 'in sync'],
  mount(el) {
    el.innerHTML = '<div id="c"></div><h3>Device state (latest first)</h3><input type="text" id="q" placeholder="filter device…"><div id="t"></div>';
  },
  update(el, node) {
    const s = node.snapshot, q = el.querySelector('#q').value.trim();
    el.querySelector('#c').innerHTML = kv({ watched: s.watched, 'with state': s.devices_with_state, behind: s.behind, 'poll errors': s.poll_errors });
    const rows = s.devices.filter(d => d.device_id.includes(q));
    el.querySelector('#t').innerHTML = `<table><tr><th>device</th><th>max_seq</th><th>acked</th><th></th></tr>${rows.map(d =>
      `<tr><td>${esc(d.device_id)}</td><td>${d.max_seq}</td><td>${d.expected}</td><td class="${d.max_seq >= d.expected ? 'ok' : 'warn'}">${d.max_seq >= d.expected ? 'landed' : 'pending'}</td></tr>`).join('')}</table>`;
  },
  onEvent() {},
};
