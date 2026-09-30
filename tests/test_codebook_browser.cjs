/* Exercise the shipped script's event handlers without a browser dependency.
 * This fixture supplies only the DOM operations the static codebook uses; it
 * reads the real HTML, runs the complete JS unchanged, and controls networking.
 */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'docs/assets/codebook.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'docs/provisional/index.html'), 'utf8');

class Element {
  constructor(tag, document) {
    this.tagName = tag.toUpperCase(); this.document = document; this.children = [];
    this.dataset = {}; this.attributes = {}; this.listeners = new Map(); this.value = '';
    this.scrollTop = 0; this._text = ''; this.className = '';
    this.classList = {
      contains: (name) => this.className.split(/\s+/).includes(name),
      add: (name) => { if (!this.classList.contains(name)) this.className += ` ${name}`; },
      remove: (name) => { this.className = this.className.split(/\s+/).filter((x) => x !== name).join(' '); }
    };
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map((x) => x.textContent).join(''); }
  get isConnected() { return this === this.document.body || Boolean(this.parent?.isConnected); }
  append(...items) {
    for (let item of items) {
      if (typeof item === 'string') item = this.document.createTextNode(item);
      if (item.tagName === '#FRAGMENT') { this.append(...item.children.slice()); continue; }
      item.remove(); item.parent = this; this.children.push(item);
    }
  }
  replaceChildren(...items) { this.children.forEach((x) => { x.parent = null; }); this.children = []; this._text = ''; this.append(...items); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((x) => x !== this); this.parent = null; }
  setAttribute(name, value) {
    this.attributes[name] = String(value);
    if (name === 'class') this.className = value;
    if (['id', 'href', 'value'].includes(name)) this[name] = value;
    if (name.startsWith('data-')) this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
    if (['hidden', 'disabled', 'open', 'checked'].includes(name)) this[name] = true;
  }
  getAttribute(name) { return this.attributes[name] ?? null; }
  addEventListener(name, handler) { if (!this.listeners.has(name)) this.listeners.set(name, []); this.listeners.get(name).push(handler); }
  dispatchEvent(event) { event.target ||= this; for (const handler of this.listeners.get(event.type) || []) handler(event); return !event.defaultPrevented; }
  click() { const event = makeEvent('click'); this.dispatchEvent(event); return event; }
  focus() { if (!this.disabled) this.document.activeElement = this; }
  select() {} scrollIntoView() {}
  matches(selector) {
    if (selector.startsWith('.')) return this.classList.contains(selector.slice(1));
    if (selector.startsWith('#')) return this.id === selector.slice(1);
    return this.tagName === selector.toUpperCase();
  }
  querySelectorAll(selector) {
    const nodes = this.children.flatMap((child) => [child, ...child.descendants()]);
    return nodes.filter((node) => selector.split(',').some((part) => {
      const bits = part.trim().split(/\s+/); if (!node.matches(bits.pop())) return false;
      let parent = node.parent;
      while (bits.length) { const match = bits.pop(); while (parent && !parent.matches(match)) parent = parent.parent; if (!parent) return false; parent = parent.parent; }
      return true;
    }));
  }
  descendants() { return this.children.flatMap((child) => [child, ...child.descendants()]); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector); }
}
function makeEvent(type, fields = {}) { return { type, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...fields }; }
function documentFixture() {
  const document = { createElement: (tag) => new Element(tag, document) };
  document.body = document.createElement('body'); document.activeElement = document.body;
  document.createTextNode = (text) => { const node = document.createElement('#text'); node.textContent = text; return node; };
  document.createDocumentFragment = () => document.createElement('#fragment');
  document.getElementById = (id) => document.body.querySelector(`#${id}`);
  document.querySelector = (selector) => document.body.querySelector(selector);
  document.querySelectorAll = (selector) => document.body.querySelectorAll(selector);
  document.addEventListener = (...args) => document.body.addEventListener(...args);
  document.dispatchEvent = (event) => document.body.dispatchEvent(event);
  const body = html.match(/<body>([\s\S]*)<\/body>/)[1];
  const stack = [document.body]; const voids = new Set(['input', 'br', 'meta', 'link', 'img']);
  for (const token of body.match(/<!--[^]*?-->|<[^>]+>|[^<]+/g)) {
    if (token.startsWith('<!--')) continue;
    if (token.startsWith('</')) { stack.pop(); continue; }
    if (token.startsWith('<')) {
      const tag = token.match(/^<([\w-]+)/)[1]; const node = document.createElement(tag);
      for (const attr of token.slice(tag.length + 1, -1).matchAll(/([\w-]+)(?:="([^"]*)")?/g)) node.setAttribute(attr[1], attr[2] ?? '');
      stack.at(-1).append(node); if (!voids.has(tag) && !token.endsWith('/>')) stack.push(node);
    } else stack.at(-1).append(document.createTextNode(token));
  }
  return document;
}
const detail = (name, extra = {}) => ({ name, label: `${name} label`, sources: ['SFA'], observed_years: [2023, 2024], storage_type: 'int64', null_count: 0,
  definitions: [{ years: [2023], label: `${name} in 2023`, description: 'Earlier definition' }, { years: [2024], label: `${name} in 2024`, description: 'Current definition' }],
  source_records: [], codes: [], ...extra });
const index = (names = ['A']) => ({ schema_version: 1, release: 'test-release', years: [2023, 2024], sources: ['SFA'], column_count: names.length, row_count: 2,
  variables: names.map((name, i) => ({ ...detail(name), detail_file: `variables-${i}.json` })) });
const response = (data) => ({ ok: true, json: async () => data });
async function flush() { await new Promise(setImmediate); }
function harness({ names = ['A'], records = {}, fetch: customFetch, hash = '#variable=A', mobile = false } = {}) {
  const document = documentFixture(), timers = new Map(), events = new Map(), requests = []; let timerId = 0;
  const location = { hash, pathname: '/provisional/', search: '', href: `https://example.test/provisional/${hash}` };
  const history = {};
  for (const key of ['pushState', 'replaceState']) history[key] = (_, __, url) => { location.hash = url.includes('#') ? url.slice(url.indexOf('#')) : ''; location.href = `https://example.test${url}`; };
  const window = { matchMedia: () => ({ matches: mobile }), addEventListener: (name, fn) => events.set(name, fn), print() {} };
  const context = { document, window, location, history, navigator: {}, Intl, URL, URLSearchParams, Blob, AbortController,
    Event: class { constructor(type) { Object.assign(this, makeEvent(type)); } },
    setTimeout: (fn, delay) => { timers.set(++timerId, { fn, delay }); return timerId; }, clearTimeout: (id) => timers.delete(id),
    fetch: async (url, options) => {
      requests.push(url);
      if (customFetch) { const override = customFetch(url, options); if (override !== undefined) return override; }
      if (url.endsWith('index.json')) return response(index(names));
      const i = Number(url.match(/variables-(\d+)/)?.[1]); const name = names[i];
      return response({ [name]: records[name] || detail(name) });
    }
  };
  vm.createContext(context); vm.runInContext(source, context, { filename: 'codebook.js' });
  return { document, location, requests, timers, events,
    get: (id) => document.getElementById(id),
    fireTimers: (delay) => { for (const [id, timer] of [...timers]) if (timer.delay === delay) { timers.delete(id); timer.fn(); } },
    button: (label) => document.querySelectorAll('button').find((node) => node.textContent === label)
  };
}

test('mobile skip link reveals search and preserves the selected variable and filters', async () => {
  const page = harness({ mobile: true, hash: '#variable=A&year=2024&tab=sources' }); await flush();
  page.get('search').value = 'A';
  assert.equal(page.get('explorer').classList.contains('detail-open'), true);
  const event = page.document.querySelector('.skip-link').click();
  assert.equal(event.defaultPrevented, true);
  assert.equal(page.get('explorer').classList.contains('detail-open'), false);
  assert.equal(page.document.activeElement, page.get('search'));
  assert.equal(page.get('search').value, 'A'); assert.equal(page.get('year-filter').value, '2024');
  assert.equal(page.get('detail-heading').textContent, 'A'); assert.match(page.location.hash, /tab=sources/);
});

test('search keyboard shortcut does not intercept typing while loading is disabled', () => {
  const page = harness({ fetch: () => new Promise(() => {}) });
  const event = makeEvent('keydown', { key: '/' }); page.document.dispatchEvent(event);
  assert.equal(event.defaultPrevented, false); assert.equal(page.document.activeElement, page.document.body);
});

test('Stata mapping displays native export labels and does not substitute source meanings', async () => {
  const page = harness({ records: { A: detail('A', { stata: {
    source_code_map: [{ source_code: 'Y', export_code: 1, label: 'Original source meaning' }, { source_code: 'N', export_code: 2, label: 'Another source meaning' }, { source_code: 'U', export_code: 3, label: 'Unassigned source meaning' }],
    value_labels: { 1: 'Native export label [2023-2024]', 2: '' }
  } }) } }); await flush();
  const text = page.get('panel-stata').textContent;
  assert.match(text, /Native export label \[2023-2024\]/); assert.match(text, /No native value label assigned/);
  assert.doesNotMatch(text, /Original source meaning|Another source meaning|Unassigned source meaning/);
  const rows = page.get('panel-stata').querySelector('tbody').children;
  assert.equal(rows[1].children[2].textContent, 'Not supplied');
  assert.equal(rows[2].children[2].textContent, 'No native value label assigned');
});

for (const badShard of [{}, { A: detail('WRONG_VARIABLE') }, { A: { name: 'A' } }]) {
  test(`invalid shard content can be retried with a fresh request: ${JSON.stringify(badShard).slice(0, 45)}`, async () => {
    let attempts = 0;
    const page = harness({ fetch: (url) => url.includes('variables-') ? response(++attempts === 1 ? badShard : { A: detail('A') }) : undefined });
    await flush(); assert.match(page.get('variable-detail').textContent, /Could not load A/);
    page.button('Try again').click(); await flush();
    assert.equal(attempts, 2); assert.equal(page.get('detail-heading').textContent, 'A');
    assert.equal(page.get('variable-detail').getAttribute('aria-busy'), 'false');
  });
}

for (const stage of ['headers', 'body']) {
  test(`a stalled ${stage} read becomes a recoverable timeout`, async () => {
    let attempts = 0;
    const page = harness({ fetch: (url, { signal }) => {
      if (!url.includes('variables-') || ++attempts > 1) return undefined;
      const waitForAbort = () => new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('Aborted'))));
      return stage === 'headers' ? waitForAbort() : { ok: true, json: waitForAbort };
    } });
    await flush(); page.fireTimers(15000); await flush();
    assert.match(page.get('variable-detail').textContent, /request timed out/);
    assert.equal(page.get('variable-detail').getAttribute('aria-busy'), 'false');
    page.button('Try again').click(); await flush(); assert.equal(page.get('detail-heading').textContent, 'A');
  });
}

test('a late old shard cannot replace the most recent selected variable', async () => {
  let resolveOld;
  const page = harness({ names: ['A', 'B'], fetch: (url) => url.endsWith('variables-0.json') ? new Promise((resolve) => { resolveOld = resolve; }) : undefined });
  await flush();
  page.get('variable-list').querySelectorAll('button').find((node) => node.dataset.name === 'B').click(); await flush();
  assert.equal(page.get('detail-heading').textContent, 'B');
  resolveOld(response({ A: detail('A') })); await flush();
  assert.equal(page.get('detail-heading').textContent, 'B'); assert.match(page.location.hash, /variable=B/);
});

test('a stalled index body leaves a clear reload path and static downloads available', async () => {
  const page = harness({ fetch: (url, { signal }) => url.endsWith('index.json') ? {
    ok: true, json: () => new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('Aborted'))))
  } : undefined });
  await flush(); page.fireTimers(15000); await flush();
  assert.equal(page.get('load-error').hidden, false);
  assert.match(page.get('load-error').textContent, /request timed out/);
  assert.ok(page.button('Reload codebook')); assert.equal(page.get('search').disabled, true);
  assert.ok(page.document.querySelector('.pdf-link').getAttribute('href').endsWith('.pdf'));
  assert.equal(page.get('variable-list').getAttribute('aria-busy'), 'false');
});


test('failed nested detail rendering cannot poison later filter changes or retry', async () => {
  let attempts = 0;
  const page = harness({ fetch: (url) => url.includes('variables-') ? response({
    A: ++attempts === 1 ? detail('A', { definitions: [null] }) : detail('A')
  }) : undefined });
  await flush(); assert.match(page.get('variable-detail').textContent, /Could not load A/);
  page.get('year-filter').value = '2024';
  assert.doesNotThrow(() => page.get('year-filter').dispatchEvent(makeEvent('change')));
  page.button('Try again').click(); await flush();
  assert.equal(attempts, 2); assert.equal(page.get('detail-heading').textContent, 'A');
  assert.match(page.get('panel-definition').textContent, /Current definition/);
});
