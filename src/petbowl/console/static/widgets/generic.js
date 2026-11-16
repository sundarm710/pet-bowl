// Fallback widget: any node type without its own widget still shows up.
export const esc = s => String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
export const kv = obj => `<div class="kv">${Object.entries(obj).map(([k, v]) => `<b>${esc(k)}</b><span>${esc(v)}</span>`).join('')}</div>`;

export default {
  summary: snap => [snap.health, ''],
  mount(el) { el.innerHTML = '<pre></pre>'; },
  update(el, node) { el.querySelector('pre').textContent = JSON.stringify(node.snapshot, null, 2); },
  onEvent() {},
};
