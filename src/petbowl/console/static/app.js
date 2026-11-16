import { Graph } from './graph.js';
import generic from './widgets/generic.js';

const $ = s => document.querySelector(s);
const widgets = {};          // node type -> widget (lazy-loaded, falls back to generic)
const mounted = {};          // node id -> bool
let topo = null, selected = null, events = [], hlCorr = null;

async function widgetFor(type) {
  if (!widgets[type]) widgets[type] = await import(`./widgets/${type}.js`).then(m => m.default, () => generic);
  return widgets[type];
}

const ctx = {
  action: (id, name, body) => fetch(`/api/nodes/${id}/actions/${name}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body || {}),
  }).then(r => r.json()),
};

const graph = new Graph($('#graph'), id => select(id));

async function refresh() {
  const t = await fetch('/api/topology').then(r => r.json());
  const shape = JSON.stringify([t.nodes.map(n => [n.id, n.type]), t.edges]);
  if (!topo || topo.shape !== shape) { t.shape = shape; graph.build(t); }
  else t.shape = shape;
  topo = t;
  const summaries = {};
  for (const n of t.nodes) summaries[n.id] = (await widgetFor(n.type)).summary(n.snapshot);
  graph.update(t.nodes, summaries);
  if (selected) await renderDrawer(false);
  const ing = t.nodes.find(n => n.type === 'ingest');
  if (ing) $('#strip').textContent = `${ing.snapshot.rate_per_s} items/s · acked ${ing.snapshot.counters.accepted} · dup ${ing.snapshot.counters.duplicates} · rej ${ing.snapshot.counters.rejected}`;
}

async function select(id) {
  selected = id; graph.select(id); mounted[id] = false;
  await renderDrawer(true); renderTail(); refresh();
}

async function renderDrawer(force) {
  const n = topo.nodes.find(x => x.id === selected); if (!n) return;
  const w = await widgetFor(n.type), body = $('#drawer-body');
  $('#drawer-title').textContent = `${n.label} · ${n.type}`;
  if (force || !mounted[n.id]) { body.className = ''; body.innerHTML = ''; w.mount(body, n, ctx); mounted[n.id] = true; }
  w.update(body, n);
}

function renderTail() {
  const only = $('#filter-node').checked;
  const list = events.filter(e => !only || !selected || e.node_id === selected).slice(-80).reverse();
  $('#tail').innerHTML = list.map(e => {
    const d = e.data;
    const brief = d.items != null ? `${d.items} items`
      : d.status != null ? `${d.status} ${JSON.stringify(d.ack || {})}`
      : d.count != null ? `${d.count} ${e.kind === 'log.dead' ? 'dead-lettered' : 'acked'}`
      : d.delta != null ? `+${d.delta} msgs`
      : d.lag != null ? `${d.group} lag ${d.lag}`
      : d.line ? d.line : d.rows != null ? `rows ${d.rows}`
      : d.device_id ? `${d.device_id} seq ${d.max_seq}` : '';
    return `<li data-corr="${e.corr || ''}" class="${hlCorr && e.corr === hlCorr ? 'hl' : ''}"><span class="n">${e.node_id}</span> <span class="k">${e.kind}</span> ${brief}${e.corr ? ` <span class="muted">${e.corr}</span>` : ''}</li>`;
  }).join('');
}

// Corr journey: highlight an event across nodes (batch.sent lists corrs; kv.state has corr).
$('#tail').addEventListener('click', ev => {
  const li = ev.target.closest('li'); if (!li) return;
  hlCorr = li.dataset.corr || null; renderTail();
});
$('#filter-node').addEventListener('change', renderTail);

function connect() {
  const es = new EventSource('/api/stream');
  es.onopen = () => { $('#conn').textContent = 'live'; $('#conn').classList.add('on'); };
  es.onerror = () => { $('#conn').textContent = 'reconnecting…'; $('#conn').classList.remove('on'); };
  let queued = false;
  es.onmessage = m => {
    const ev = JSON.parse(m.data);
    events.push(ev); if (events.length > 1000) events.splice(0, 200);
    graph.pulse(ev.node_id, ev.kind.startsWith('log.')); // log.* = simulator -> ingest traffic seen via files
    if (ev.kind === 'batch.sent') graph.pulse('ingest');
    if (!queued) { queued = true; setTimeout(() => { queued = false; renderTail(); }, 250); }
    widgetFor(ev.node_type).then(w => { const b = $('#drawer-body'); if (selected === ev.node_id) w.onEvent(b, ev); });
  };
}

await refresh();
connect();
setInterval(refresh, 1000);
