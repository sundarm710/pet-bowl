import { esc, kv } from './generic.js';

const spark = (xs) => {
  if (xs.length < 2) return '';
  const max = Math.max(...xs, 1), w = 300, h = 34;
  const pts = xs.map((v, i) => `${(i / (xs.length - 1)) * w},${h - (v / max) * (h - 2) - 1}`).join(' ');
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><polyline points="${pts}"/></svg>`;
};

export default {
  summary: s => s.health === 'down'
    ? ['down', s.error || '']
    : [`${s.health} · ${s.end_total} msgs`, `${s.rate_per_s}/s · lag ${s.groups.length ? s.total_lag : '–'}`],
  mount(el, node, ctx) {
    el.innerHTML = `<div id="c"></div><h3>Produce rate (msgs/s, last 60s)</h3><div id="sp"></div>
      <h3>Partitions</h3><div id="p"></div><h3>Consumer groups</h3><div id="g"></div>
      <h3>Peek <span><button id="peek">Last 10 per partition</button></span></h3><div id="m" class="muted">Reads without joining a group or committing.</div>`;
    el.querySelector('#peek').onclick = async () => {
      const m = el.querySelector('#m'); m.textContent = 'reading…';
      const r = await ctx.action(node.id, 'peek', { n: 10 });
      if (!r.ok) { m.innerHTML = `<span class="bad">${esc(r.error)}</span>`; return; }
      m.innerHTML = r.messages.length ? `<table><tr><th>p</th><th>offset</th><th>key</th><th>event</th></tr>${r.messages.map(x =>
        `<tr><td>${x.partition}</td><td>${x.offset}</td><td>${esc(x.key)}</td><td>${esc(typeof x.value === 'string' ? x.value : `${x.value.kind} ${x.value.value ?? ''} seq ${x.value.seq}`)}</td></tr>`).join('')}</table>` : 'topic is empty';
    };
  },
  update(el, node) {
    const s = node.snapshot;
    el.querySelector('#c').innerHTML = kv({
      health: s.health, ...(s.error ? { error: s.error } : {}), broker: s.bootstrap, topic: s.topic,
      'topic exists': s.topic_exists, 'msgs/s': s.rate_per_s, 'total offsets': s.end_total,
      retained: s.retained, 'produced since console start': s.produced_since_start,
    });
    el.querySelector('#sp').innerHTML = spark(s.rate_history);
    el.querySelector('#p').innerHTML = s.partitions.length ? `<table><tr><th>p</th><th>low</th><th>high</th><th>msgs</th></tr>${s.partitions.map(p =>
      `<tr><td>${p.partition}</td><td>${p.low}</td><td>${p.high}</td><td>${p.count}</td></tr>`).join('')}</table>` : '<span class="muted">none</span>';
    el.querySelector('#g').innerHTML = s.groups.length ? `<table><tr><th>group</th><th>lag</th><th>per partition</th></tr>${s.groups.map(g =>
      `<tr><td>${esc(g.group)}</td><td class="${g.lag ? 'warn' : 'ok'}">${g.lag}</td><td>${Object.entries(g.partitions).map(([p, v]) => `${p}:${v.lag}`).join(' ')}</td></tr>`).join('')}</table>` : '<span class="muted">no consumer groups yet (consumers are not built)</span>';
  },
  onEvent() {},
};
