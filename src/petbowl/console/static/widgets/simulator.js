import { esc, kv } from './generic.js';

export default {
  summary: s => [s.health, s.pid && s.uptime_s ? `pid ${s.pid} · ${s.uptime_s}s` : s.external.active ? `terminal run · ${s.external.acked} acked` : (s.exit_code != null ? `exit ${s.exit_code}` : '')],
  mount(el, node, ctx) {
    const sch = node.snapshot.param_schema;
    el.innerHTML = `<form class="params">${Object.entries(sch).map(([k, v]) =>
      `<label>${k}<input type="number" step="any" name="${k}" value="${node.snapshot.params[k] ?? v.default}"></label>`).join('')}</form>
      <button id="go">Start</button> <button id="stop" class="stop">Stop</button>
      <span id="msg" class="muted"></span><h3>Terminal runs (tailing log files)</h3><div id="ext"></div><h3>Simulator log</h3><pre id="log"></pre>`;
    const params = () => Object.fromEntries([...el.querySelectorAll('input')].map(i => [i.name, Number(i.value)]));
    const msg = el.querySelector('#msg');
    el.querySelector('#go').onclick = async () => {
      const r = await ctx.action('simulator', 'start', params());
      msg.textContent = r.ok ? 'started' : r.error;
    };
    el.querySelector('#stop').onclick = async () => { await ctx.action('simulator', 'stop', {}); msg.textContent = 'stopped'; };
  },
  update(el, node) {
    const s = node.snapshot, run = s.pid != null && s.uptime_s > 0;
    const x = s.external;
    el.querySelector('#ext').innerHTML = kv({ 'ack log': x.ack_log || 'off', 'dead-letter': x.dead_letter || 'off', active: x.active, acked: x.acked, 'dead-lettered': x.dead_lettered, ...Object.fromEntries(Object.entries(x.dead_reasons).map(([k, v]) => [`dl: ${k}`, v])) });
    el.querySelector('#go').disabled = run;
    el.querySelector('#stop').disabled = !run;
    el.querySelector('#log').textContent = s.logs.join('\n');
  },
  onEvent() {},
};
