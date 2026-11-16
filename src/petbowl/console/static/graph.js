// Pipeline graph: layered layout derived from edges, rendered as SVG.
const NS = 'http://www.w3.org/2000/svg';
const W = 170, H = 64, GX = 90, GY = 28;

function el(tag, attrs = {}, parent) {
  const e = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

export function layout(nodes, edges) {
  const depth = {};
  nodes.forEach(n => depth[n.id] = 0);
  for (let i = 0; i < nodes.length; i++)
    for (const e of edges) depth[e.dst] = Math.max(depth[e.dst], depth[e.src] + 1);
  const cols = {};
  nodes.forEach(n => (cols[depth[n.id]] ||= []).push(n.id));
  const pos = {};
  for (const [d, ids] of Object.entries(cols))
    ids.forEach((id, i) => pos[id] = { x: 30 + d * (W + GX), y: 30 + i * (H + GY) });
  return pos;
}

export class Graph {
  constructor(svg, onSelect) {
    this.svg = svg; this.onSelect = onSelect; this.sel = null;
    this.edgeEls = []; this.nodeEls = {};
  }
  build(topo) {
    this.svg.replaceChildren();
    this.topo = topo;
    const pos = this.pos = layout(topo.nodes, topo.edges);
    this.edgeEls = topo.edges.map(e => {
      const a = pos[e.src], b = pos[e.dst];
      const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2, mx = (x1 + x2) / 2;
      const p = el('path', { class: e.planned ? 'edge planned' : 'edge', d: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}` }, this.svg);
      const t = el('text', { class: 'edge-label', x: mx, y: (y1 + y2) / 2 - 6, 'text-anchor': 'middle' }, this.svg);
      t.textContent = e.label;
      return { e, p };
    });
    for (const n of topo.nodes) {
      const { x, y } = pos[n.id];
      const g = el('g', { class: 'node', transform: `translate(${x},${y})` }, this.svg);
      el('rect', { width: W, height: H }, g);
      el('circle', { class: 'dot', cx: 14, cy: 16 }, g);
      const t = el('text', { class: 't', x: 28, y: 20 }, g); t.textContent = n.label;
      const s = el('text', { class: 's', x: 14, y: 42 }, g);
      const s2 = el('text', { class: 's', x: 14, y: 56 }, g);
      g.addEventListener('click', () => this.onSelect(n.id));
      this.nodeEls[n.id] = { g, s, s2 };
    }
    this.size(pos);
  }
  size(pos) {
    const xs = Object.values(pos);
    this.svg.setAttribute('viewBox', `0 0 ${Math.max(...xs.map(p => p.x)) + W + 30} ${Math.max(...xs.map(p => p.y)) + H + 30}`);
  }
  update(nodes, summaries) {
    for (const n of nodes) {
      const ne = this.nodeEls[n.id]; if (!ne) continue;
      ne.g.setAttribute('class', `node h-${n.snapshot.health}${this.sel === n.id ? ' sel' : ''}`);
      const [a, b] = summaries[n.id] || ['', ''];
      ne.s.textContent = a; ne.s2.textContent = b;
    }
  }
  select(id) { this.sel = id; }
  pulse(nodeId, outgoing = false) {
    for (const { e, p } of this.edgeEls) if ((outgoing ? e.src : e.dst) === nodeId) {
      p.classList.add('pulse'); setTimeout(() => p.classList.remove('pulse'), 150);
    }
  }
}
