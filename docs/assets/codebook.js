(() => {
  'use strict';
  const byId = (id) => document.getElementById(id);
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  };
  const number = (value) => new Intl.NumberFormat('en-US').format(value);
  const readable = (value) => String(value).replaceAll('_', ' ').replace(/^./, (c) => c.toUpperCase());
  const values = (value) => value === null || value === undefined || value === '' ? 'Not supplied' :
    (typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value));
  function yearsLabel(input) {
    const years = [...new Set((input || []).map(Number))].filter(Number.isFinite).sort((a, b) => a - b);
    const ranges = [];
    for (let i = 0; i < years.length; i++) {
      const start = years[i];
      let end = start;
      while (i + 1 < years.length && years[i + 1] === end + 1) end = years[++i];
      ranges.push(start === end ? String(start) : `${start}–${end}`);
    }
    return ranges.join(', ') || 'Not supplied';
  }
  const state = { index: null, matches: [], selected: null, detail: null, limit: 100, tab: 'definition', token: 0, aliases: new Map() };
  const tabNames = ['definition', 'codes', 'sources', 'stata'];
  let searchTimer;
  const cache = new Map();
  const controls = ['search', 'year-filter', 'source-filter', 'code-filter', 'gap-filter', 'reset-filters', 'download-filtered'];
  const selectedYear = () => Number(byId('year-filter').value) || null;
  const inYear = (record) => !selectedYear() || (record.years || []).includes(selectedYear());
  const isMobile = () => window.matchMedia('(max-width: 780px)').matches;
  const announce = (message) => { byId('announcement').textContent = message; };
  function button(text, className, action) {
    const node = el('button', className, text);
    node.type = 'button';
    node.addEventListener('click', action);
    return node;
  }
  function addField(dl, name, value) {
    dl.append(el('dt', '', name), el('dd', value === null || value === undefined || value === '' ? 'blank-value' : '', values(value)));
  }
  function addSourceField(dl, name, value) {
    if (typeof value === 'string' && /^https?:\/\//i.test(value)) {
      try {
        const url = new URL(value);
        if (url.protocol === 'http:' || url.protocol === 'https:') {
          const link = el('a', 'source-link', value);
          link.href = url.href;
          const field = el('dd'); field.append(link);
          dl.append(el('dt', '', name), field);
          return;
        }
      } catch (_) { /* Preserve malformed source text without creating a link. */ }
    }
    addField(dl, name, value);
  }
  function table(headers, rows, classes = []) {
    const wrap = el('div', 'data-table-wrap');
    const node = el('table', 'data-table');
    const head = el('thead');
    const tr = el('tr');
    headers.forEach((label) => { const th = el('th', '', label); th.scope = 'col'; tr.append(th); });
    head.append(tr);
    const body = el('tbody');
    rows.forEach((row) => {
      const line = el('tr');
      row.forEach((value, i) => line.append(el('td', classes[i] || '', values(value))));
      body.append(line);
    });
    node.append(head, body);
    wrap.append(node);
    return wrap;
  }
  function makeTechnical(title, value) {
    const details = el('details', 'technical-details');
    details.append(el('summary', '', title), el('pre', '', JSON.stringify(value, null, 2)));
    return details;
  }
  async function fetchJson(url) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response.ok) throw new Error(`The codebook file could not be loaded (HTTP ${response.status}).`);
      // Keep the timeout active while the response body is being read, too.
      return await response.json();
    } catch (error) {
      if (controller.signal.aborted) throw new Error('The codebook request timed out. Please try again.');
      throw error;
    } finally { clearTimeout(timeout); }
  }
  function downloadPath(name) {
    if (typeof name !== 'string' || !/^[a-zA-Z0-9._-]+$/.test(name)) return null;
    return `./codebook/${name}`;
  }
  // RFC 4180 field quoting, including embedded commas, newlines and doubled quotes.
  function parseCsv(text) {
    const rows = []; let row = [], field = '', quoted = false, closed = false;
    text = text.replace(/^\uFEFF/, '');
    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (quoted) {
        if (char === '"' && text[i + 1] === '"') { field += '"'; i++; }
        else if (char === '"') { quoted = false; closed = true; }
        else field += char;
      } else if (char === '"') {
        if (field || closed) throw new Error('Invalid quoted crosswalk field.');
        quoted = true;
      } else if (char === ',' || char === '\r' || char === '\n') {
        row.push(field); field = ''; closed = false;
        if (char !== ',') {
          rows.push(row); row = [];
          if (char === '\r' && text[i + 1] === '\n') i++;
        }
      } else {
        if (closed) throw new Error('Invalid text after a quoted crosswalk field.');
        field += char;
      }
    }
    if (quoted) throw new Error('Unclosed crosswalk field.');
    if (field || closed || row.length) { row.push(field); rows.push(row); }
    return rows;
  }
  async function loadAliases() {
    const filename = (state.index.downloads || {}).column_crosswalk;
    if (!filename) return;
    const note = byId('search-status') || el('p', 'search-note');
    note.textContent = 'Loading original column names…'; note.setAttribute('role', 'status');
    if (!note.isConnected) { note.id = 'alias-search-note'; byId('search').closest('.search-toolbar').after(note); }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const path = downloadPath(filename);
      if (!path) throw new Error('Invalid crosswalk filename.');
      const response = await fetch(path, { signal: controller.signal });
      if (!response.ok) throw new Error('Crosswalk unavailable.');
      const rows = parseCsv(await response.text());
      const headers = rows.shift() || [];
      const canonical = headers.indexOf('canonical_name'), original = headers.indexOf('original_column');
      if (canonical < 0 || original < 0 || !rows.length) throw new Error('Invalid crosswalk columns.');
      const names = new Set(state.index.variables.map((item) => item.name));
      const aliases = new Map(), owners = new Map();
      rows.forEach((row) => {
        if (row.length !== headers.length || !names.has(row[canonical]) || !row[original]) throw new Error('Invalid crosswalk record.');
        if (owners.has(row[original]) && owners.get(row[original]) !== row[canonical]) throw new Error('Ambiguous original column.');
        owners.set(row[original], row[canonical]);
        if (!aliases.has(row[canonical])) aliases.set(row[canonical], []);
        aliases.get(row[canonical]).push(row[original]);
      });
      state.aliases = aliases;
      note.textContent = 'Original column names are searchable.';
      if (byId('search').value.trim()) applyFilters();
    } catch (_) {
      note.textContent = 'Original column lookup is unavailable. Search still covers current names, labels and sources.';
    } finally { clearTimeout(timeout); }
  }
  function configureDownloads() {
    const mappings = { dictionary: 'codebook.csv', definitions: 'definitions.csv.gz', value_labels: 'value-labels.csv', pdf: 'ipeds-panel-codebook.pdf' };
    Object.entries(mappings).forEach(([key, fallback]) => {
      const filename = (state.index.downloads || {})[key] || fallback;
      const path = downloadPath(filename);
      if (!path) return;
      const oldPaths = [`./codebook/${fallback}`, `./codebook/${fallback}.gz`];
      document.querySelectorAll('a').forEach((link) => {
        if (oldPaths.includes(link.getAttribute('href'))) {
          link.href = path;
          link.download = `${state.index.release}-${filename}`;
          if (filename.endsWith('.gz') && !/gzip|compressed/i.test(link.textContent)) link.append(document.createTextNode(' (gzip)'));
        }
      });
    });
  }
  function renderRelease() {
    const index = state.index;
    byId('stat-variables').textContent = number(index.column_count);
    byId('stat-rows').textContent = number(index.row_count);
    byId('stat-years').textContent = yearsLabel(index.years);
    byId('release-name').textContent = index.release;
    const content = byId('release-content');
    content.replaceChildren();
    content.append(el('p', '', `Release: ${index.release}. Panel keys: ${(index.panel_keys || []).join(' + ')}.`));
    (index.notes || []).forEach((note) => content.append(el('p', '', note)));
    if (index.downloads && index.downloads.column_crosswalk) {
      const link = byId('download-crosswalk') || el('a', 'text-link', 'Original column names (CSV)');
      link.href = `./codebook/${index.downloads.column_crosswalk}`;
      link.download = `${index.release}-${index.downloads.column_crosswalk}`;
      link.hidden = false;
      if (!link.isConnected) content.append(link);
    }
    const gapVariables = index.variables.filter((item) => item.has_issues);
    if (gapVariables.length) {
      const notice = el('div', 'gap-callout');
      notice.append(el('strong', '', `${number(gapVariables.length)} variables have documented metadata gaps.`));
      notice.append(el('p', '', 'Use the Metadata gaps filter to find these variables. Labels from another year do not resolve a missing definition.'));
      content.append(notice);
    }
    const hashes = el('details', 'technical-details');
    hashes.append(el('summary', '', 'Source file fingerprints (SHA-256)'));
    const fields = el('dl', 'hash-grid');
    Object.entries(index.generated_from || {}).forEach(([key, value]) => addField(fields, readable(key), value));
    hashes.append(fields);
    content.append(hashes);
    configureDownloads();
  }
  function renderList() {
    const list = byId('variable-list');
    list.replaceChildren();
    byId('result-count').textContent = number(state.matches.length);
    byId('list-range').textContent = state.matches.length ? `Showing ${number(Math.min(state.limit, state.matches.length))} of ${number(state.matches.length)}` : 'No matching variables';
    byId('show-more').hidden = state.matches.length <= state.limit;
    byId('download-filtered').disabled = !state.matches.length;
    if (byId('download-matching-count')) byId('download-matching-count').textContent = number(state.matches.length);
    if (!state.matches.length) {
      const empty = el('div', 'empty-results');
      empty.append(el('strong', '', 'No matching variables'), el('p', '', 'Try a shorter search or broaden the source and year filters.'), button('Clear all filters', '', resetFilters));
      list.append(empty);
      return;
    }
    const fragment = document.createDocumentFragment();
    state.matches.slice(0, state.limit).forEach((item) => {
      const row = button('', 'variable-item', () => selectVariable(item.name, { navigate: true, focus: true, openMobile: true }));
      row.dataset.name = item.name;
      row.setAttribute('aria-current', String(state.selected === item.name));
      row.setAttribute('aria-label', `${item.name}: ${item.label}${item.has_issues ? '. Metadata gaps' : ''}`);
      const top = el('span', 'variable-top');
      top.append(el('span', 'variable-name', item.name));
      const icons = el('span', 'variable-icons');
      if (item.has_codes) icons.append(el('span', 'small-badge', 'CODED'));
      if (item.has_issues) { const dot = el('span', 'gap-dot'); dot.title = 'Documented metadata gap'; dot.setAttribute('aria-hidden', 'true'); icons.append(dot); }
      top.append(icons);
      row.append(top, el('span', 'variable-label', item.label || 'Label not supplied'));
      const query = byId('search').value.trim().toLocaleLowerCase();
      const alias = (state.aliases.get(item.name) || []).find((name) => name.toLocaleLowerCase() === query);
      if (alias) row.append(el('span', 'variable-alias', `Previously ${alias}`));
      fragment.append(row);
    });
    list.append(fragment);
  }
  function navigationNotice(message = '') {
    let notice = byId('navigation-notice');
    if (!notice) {
      notice = el('div', 'navigation-notice'); notice.id = 'navigation-notice'; notice.setAttribute('role', 'status');
      byId('load-error').after(notice);
    }
    notice.hidden = !message;
    notice.replaceChildren();
    if (message) notice.append(el('p', '', message), button('Clear filters and browse variables', 'reset-button', resetFilters));
  }
  function applyFilters(options = {}) {
    if (!state.index) return;
    clearTimeout(searchTimer);
    if (!options.keepNotice) navigationNotice();
    const query = byId('search').value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
    const year = selectedYear();
    const source = byId('source-filter').value;
    const codes = byId('code-filter').value;
    const gaps = byId('gap-filter').checked;
    state.matches = state.index.variables.filter((item) => {
      const text = `${item.name} ${item.label} ${(item.sources || []).join(' ')} ${(state.aliases.get(item.name) || []).join(' ')}`.toLocaleLowerCase();
      return query.every((word) => text.includes(word)) && (!year || (item.observed_years || []).includes(year)) &&
        (!source || (item.sources || []).includes(source)) && (!codes || (codes === 'coded' ? item.has_codes : !item.has_codes)) && (!gaps || item.has_issues);
    });
    const exactQuery = byId('search').value.trim().toLocaleLowerCase();
    if (exactQuery) {
      const rank = (item) => item.name.toLocaleLowerCase() === exactQuery ? 0 :
        (state.aliases.get(item.name) || []).some((alias) => alias.toLocaleLowerCase() === exactQuery) ? 1 : 2;
      state.matches.sort((a, b) => rank(a) - rank(b));
    }
    state.limit = 100;
    let selectedFirst = false;
    if (state.selected) {
      const selectedPosition = state.matches.findIndex((item) => item.name === state.selected);
      if (selectedPosition >= state.limit) {
        state.matches.unshift(state.matches.splice(selectedPosition, 1)[0]);
        selectedFirst = true;
      }
    }
    document.querySelector('.list-caption').textContent = selectedFirst ? 'Selected variable first' : 'Name and label';
    renderList();
    byId('variable-list').scrollTop = 0;
    if (options.select === false) return;
    if (state.selected && state.matches.some((item) => item.name === state.selected)) {
      if (state.detail) renderDetail(state.detail);
    } else if (state.matches.length) {
      selectVariable(state.matches[0].name, { navigate: false, focus: false, openMobile: false });
    } else {
      state.selected = null;
      state.detail = null;
      state.token++;
      byId('variable-detail').setAttribute('aria-busy', 'false');
      const empty = el('div', 'detail-placeholder');
      empty.append(el('h2', '', 'No matching variables'), el('p', '', 'Clear a filter or try a different variable name.'));
      byId('variable-detail').replaceChildren(empty);
    }
    if (options.sync !== false) updateLink(state.selected, true);
  }
  function resetFilters() {
    byId('explorer').classList.remove('detail-open');
    byId('search').value = '';
    byId('year-filter').value = '';
    byId('source-filter').value = '';
    byId('code-filter').value = '';
    byId('gap-filter').checked = false;
    applyFilters();
    byId('search').focus();
  }
  function updateLink(name, replace = false) {
    const parameters = new URLSearchParams();
    if (name) parameters.set('variable', name);
    if (selectedYear()) parameters.set('year', String(selectedYear()));
    if (name && state.tab !== 'definition') parameters.set('tab', state.tab);
    const hash = parameters.size ? `#${parameters.toString()}` : '';
    if (location.hash !== hash) history[replace ? 'replaceState' : 'pushState'](null, '', `${location.pathname}${location.search}${hash}`);
  }
  function backToResults() {
    byId('explorer').classList.remove('detail-open');
    const active = Array.from(byId('variable-list').children).find((row) => row.dataset.name === state.selected);
    (active || byId('search')).focus({ preventScroll: true });
    byId('explorer').scrollIntoView({ block: 'start' });
  }
  async function selectVariable(name, options = {}) {
    const item = state.index.variables.find((candidate) => candidate.name === name);
    if (!item) return;
    navigationNotice();
    const changed = state.selected !== name;
    state.selected = name;
    state.detail = null;
    if (options.tab && tabNames.includes(options.tab)) state.tab = options.tab;
    else if (changed) state.tab = 'definition';
    const token = ++state.token;
    byId('variable-list').querySelectorAll('.variable-item').forEach((row) => row.setAttribute('aria-current', String(row.dataset.name === name)));
    if (options.navigate) updateLink(name);
    if (options.openMobile) byId('explorer').classList.add('detail-open');
    const detail = byId('variable-detail');
    detail.setAttribute('aria-busy', 'true');
    const loading = el('div', 'detail-placeholder');
    loading.append(el('h2', '', name), el('p', '', 'Loading definitions and source records…'));
    detail.replaceChildren(loading);
    let request;
    try {
      if (!/^variables-\d+\.json$/.test(item.detail_file)) throw new Error('The variable reference file is invalid.');
      if (!cache.has(item.detail_file)) cache.set(item.detail_file, fetchJson(`./codebook/${item.detail_file}`));
      request = cache.get(item.detail_file);
      const shard = await request;
      if (token !== state.token) return;
      if (!shard || typeof shard !== 'object' || Array.isArray(shard) || !Object.prototype.hasOwnProperty.call(shard, name)) throw new Error('The variable detail is missing from this release.');
      const record = shard[name];
      if (!record || record.name !== name || !Array.isArray(record.definitions) || !Array.isArray(record.source_records) || !Array.isArray(record.codes)) throw new Error('The variable detail does not match this release.');
      state.detail = record;
      renderDetail(state.detail);
      detail.scrollTop = 0;
      if (options.focus) byId('detail-heading').focus({ preventScroll: true });
      if (options.openMobile && isMobile()) detail.scrollIntoView({ block: 'start' });
      announce(`Showing ${name}. ${state.detail.label || ''}`);
    } catch (error) {
      // Do not leave the retry button attached to a cached invalid response, or
      // evict a newer request that replaced this one while it was in flight.
      if (request && cache.get(item.detail_file) === request) cache.delete(item.detail_file);
      if (token !== state.token) return;
      state.detail = null;
      const failed = el('div', 'empty-results');
      failed.append(button('← Back to variables', 'mobile-back', backToResults), el('strong', '', `Could not load ${name}`), el('p', '', error.message), button('Try again', '', () => selectVariable(name, options)));
      detail.replaceChildren(failed);
    } finally {
      if (token === state.token) detail.setAttribute('aria-busy', 'false');
    }
  }
  function renderIssues(detail) {
    if (!(detail.issues || []).length) return null;
    const box = el('aside', 'gap-callout');
    box.append(el('h3', '', 'Known metadata gaps · all years'));
    const list = el('ul');
    detail.issues.forEach((issue) => {
      const item = el('li', '', issue.message || readable(issue.code));
      if (issue.examples && issue.examples.length) {
        const examples = issue.examples.map((example) => typeof example === 'object' ? Object.entries(example).map(([key, value]) => `${readable(key)}: ${values(value)}`).join(' · ') : String(example)).join('; ');
        item.append(el('p', '', `Examples: ${examples}`));
      }
      list.append(item);
    });
    box.append(list);
    return box;
  }
  function renderDefinition(detail, panel) {
    const issue = renderIssues(detail);
    if (issue) panel.append(issue);
    panel.append(el('h3', '', selectedYear() ? `Definition · ${selectedYear()}` : 'Definitions by year'));
    const definitions = (detail.definitions || []).filter(inYear);
    if (definitions.length) {
      definitions.forEach((definition) => {
        const block = el('section', 'definition-block');
        if (!selectedYear()) block.append(el('span', 'year-tag', yearsLabel(definition.years)));
        if (!selectedYear() || definitions.length > 1) block.append(el('h4', '', definition.label || 'Label not supplied'));
        block.append(el('p', definition.description ? 'data-description' : 'blank-value', definition.description || 'Description not supplied'));
        panel.append(block);
      });
    } else panel.append(el('p', 'blank-value', selectedYear() ? `No definition is supplied for ${selectedYear()}.` : detail.description || 'No definition is supplied.'));
    const measurement = el('section', 'subsection');
    measurement.append(el('h3', '', 'Measurement metadata · full release'), el('p', 'section-intro', 'This summary covers all years. Year-specific reporting periods and populations are in Sources.'));
    const semantics = detail.semantic_metadata || {};
    const grid = el('dl', 'semantic-grid');
    ['units', 'currency', 'price_basis', 'reference_period'].forEach((key) => {
      const group = semantics[key];
      const item = el('div');
      item.append(el('dt', '', readable(key)));
      let text = 'Not supplied';
      if (group && typeof group === 'object') {
        text = group.value !== null && group.value !== undefined && group.value !== '' ? values(group.value) :
          (group.values && group.values.length ? group.values.map(values).join('; ') : 'Not supplied');
        if (group.status && group.status !== 'complete' && text !== 'Not supplied') {
          text += group.status === 'unknown' ? ' · Not documented for every source record' : ` · ${readable(group.status)}`;
        }
      } else if (group) text = values(group);
      item.append(el('dd', '', text));
      grid.append(item);
    });
    measurement.append(grid);
    if (Object.keys(semantics).length) measurement.append(makeTechnical('Inspect measurement metadata', semantics));
    panel.append(measurement);
  }
  function renderCodes(detail, panel) {
    panel.append(el('h3', '', 'Category meanings'), el('p', 'section-intro', 'Original panel codes and their documented years. The Stata tab lists any numeric codes assigned during export.'));
    const issue = renderIssues(detail);
    if (issue) panel.append(issue);
    const records = (detail.codes || []).filter(inYear);
    if (!records.length) {
      panel.append(el('p', 'blank-value', 'No category labels are documented for this year selection.'));
    } else {
      const label = el('label', 'table-search');
      label.append(document.createTextNode('Find a code'));
      const input = el('input');
      input.type = 'search'; input.placeholder = 'Code or meaning…';
      label.append(input); panel.append(label);
      const container = el('div'); panel.append(container);
      const render = () => {
        const query = input.value.trim().toLocaleLowerCase();
        const matches = records.filter((record) => `${record.code} ${record.label}`.toLocaleLowerCase().includes(query));
        container.replaceChildren(el('p', 'section-label', `${number(matches.length)} documented code meanings`));
        if (matches.length) container.append(table(['Code', 'Meaning', 'Years', 'Source'], matches.map((record) => [record.code, record.label || 'Not supplied', yearsLabel(selectedYear() ? [selectedYear()] : record.years), (record.sources || []).join(', ') || 'Not supplied']), ['code-cell', '', 'years-cell', '']));
        else container.append(el('p', '', 'No code meanings match this search.'));
      };
      input.addEventListener('input', render); render();
    }
    if ((detail.code_provenance || []).length) {
      const source = el('section', 'subsection');
      source.append(el('h3', '', 'Codebook evidence'), el('p', '', 'Verified additions retain their same-year sources and original records.'));
      const scoped = detail.code_provenance.filter(inYear);
      source.append(makeTechnical('Inspect code-label source evidence', scoped));
      if (detail.original_codes && detail.original_codes.length) source.append(makeTechnical('Preserved original code-label records', detail.original_codes.filter(inYear)));
      panel.append(source);
    }
  }
  function renderSources(detail, panel) {
    panel.append(el('h3', '', 'Source records'), el('p', 'section-intro', 'Physical source tables, reporting periods and release status. Corrections retain the original dictionary reference.'));
    if (detail.column_consolidation) {
      const rule = detail.column_consolidation;
      panel.append(el('h3', '', 'Consolidated source columns'), el('p', '', rule.rationale || 'Verified source-table moves are represented by one column. Original values and year-specific definitions are retained.'));
      panel.append(table(['Original column', 'Years'], rule.members.map((member) => [member.column, yearsLabel(member.years)])));
      (Array.isArray(rule.caveats) ? rule.caveats : [rule.caveats]).filter(Boolean).forEach((note) => panel.append(el('p', 'scope-note', note)));
    }
    const records = (detail.source_records || []).filter(inYear);
    if (!records.length) { panel.append(el('p', 'blank-value', 'No source record is supplied for this year selection.')); return; }
    records.forEach((record) => {
      const card = el('details', 'record-card');
      if (record.correction_id || records.length === 1) card.open = true;
      const summary = el('summary');
      summary.append(el('strong', '', record.table || record.source_file || 'Source not supplied'), document.createTextNode(` · ${yearsLabel(selectedYear() ? [selectedYear()] : record.years)}`));
      if (record.correction_id) summary.append(el('span', 'pill warning', ' Corrected reference'));
      card.append(summary);
      const fields = el('dl', 'record-fields');
      const definition = Number.isInteger(record.definition_index) ? (detail.definitions || [])[record.definition_index] || {} : {};
      const entries = [
        ['Reporting period', record.source_table_reference_period || record.reference_period],
        ['Reporting population', record.reporting_population_note],
        ['Release status', record.release_type], ['Release date', record.source_release_date],
        ['Physical table', record.table], ['Original dictionary table', record.original_table], ['Source component', record.source_file],
        ['Original variable', record.varname], ['Source title', record.title || definition.label],
        ['Source flag variable', record.imputationvar], ['Flag availability', record.imputation_flag_availability],
        ['Source download', record.source_url], ['Reporting instructions', record.reporting_population_source]
      ];
      entries.forEach(([name, value]) => {
        if (value !== undefined && value !== null && value !== '') addSourceField(fields, name, value);
      });
      card.append(fields);
      if (record.correction_id || record.correction_reason) {
        const note = el('div', 'correction-note');
        note.append(el('strong', '', record.correction_id || 'Documented correction'), el('p', '', record.correction_reason || 'See the preserved verification evidence in this record.'));
        card.append(note);
      }
      const evidence = el('details', 'source-evidence');
      evidence.append(el('summary', '', 'Full source record and verification evidence'));
      const provenance = el('dl', 'record-fields');
      Object.entries(record).forEach(([key, value]) => addSourceField(provenance,
        key === 'academic_year_label' ? 'Release designation' : readable(key), value));
      evidence.append(provenance); card.append(evidence);
      panel.append(card);
    });
  }
  function renderStata(detail, panel) {
    const stata = detail.stata || {};
    panel.append(el('h3', '', 'Representation in Stata'), el('p', 'section-intro', 'The labeled .dta export contains native variable and value labels. Longer definitions and source records remain in this codebook and the companion metadata.'));
    const fields = el('dl', 'record-fields');
    addField(fields, 'Stata name', stata.name || detail.stata_name || detail.name);
    addField(fields, 'Variable label', stata.label);
    addField(fields, 'Storage conversion', readable(stata.storage_conversion || 'none'));
    panel.append(fields);
    const map = stata.source_code_map || [];
    if (map.length) {
      panel.append(el('p', 'scope-note', stata.storage_conversion === 'integer_code_strings_to_numeric' ? 'Canonical integer strings become the same numeric codes in Stata. This mapping preserves the original tokens; Parquet values are unchanged.' : 'Original string categories are represented by assigned numeric Stata codes. Use this mapping to recover the original tokens; the assigned numbers are not the original panel codes.'));
      panel.append(table(['Original panel code', 'Stata code', 'Stata value label'], map.map((item) => [item.source_code, item.export_code, (stata.value_labels || {})[String(item.export_code)] ?? 'No native value label assigned']), ['code-cell', 'code-cell', '']));
    } else if (stata.storage_conversion === 'integer_code_strings_to_numeric') {
      panel.append(el('p', 'scope-note', 'Canonical integer strings become the same numeric codes in Stata. Original Parquet values are unchanged.'));
    }
    const labels = Object.entries(stata.value_labels || {});
    if (labels.length && !map.length) panel.append(table(['Stata code', 'Native value label'], labels.map(([code, label]) => [code, label]), ['code-cell', '']));
    if (!labels.length && !map.length) panel.append(el('p', 'blank-value', 'No native category labels are assigned to this variable.'));
    panel.append(el('p', 'section-intro', 'Stata labels describe the full export, so year ranges shown inside a label remain significant even when a single year is selected above. String missing values become empty strings in Stata. The full panel may exceed the variable limit of your Stata edition; select the variables you need when loading it.'));
  }
  function renderDetail(detail) {
    const container = byId('variable-detail');
    container.replaceChildren();
    container.append(button('← Back to variables', 'mobile-back', backToResults));
    const releaseContext = el('p', 'reference-context', `${yearsLabel(state.index.years)} · ${state.index.release}. ${document.querySelector('.release-status strong').textContent}`);
    container.append(releaseContext);
    const header = el('header');
    const top = el('div', 'detail-header-top');
    const badges = el('div', 'detail-kicker');
    (detail.sources || []).forEach((source) => badges.append(el('span', 'pill', source)));
    if (!detail.sources || !detail.sources.length) badges.append(el('span', 'pill', 'Panel variable'));
    if (detail.has_issues) badges.append(el('span', 'pill warning', 'Metadata gaps'));
    const actions = el('div', 'detail-actions');
    const copy = button('Copy link', '', async () => {
      updateLink(detail.name, true);
      try { await navigator.clipboard.writeText(location.href); copy.textContent = 'Link copied'; announce('Variable link copied.'); }
      catch (_) {
        const field = el('input'); field.value = location.href; field.readOnly = true; field.setAttribute('aria-label', 'Variable link to copy');
        actions.append(field); field.focus(); field.select(); copy.textContent = 'Select and copy';
      }
    });
    actions.append(copy, button('Print variable', '', () => window.print()));
    top.append(badges, actions);
    const heading = el('h2', 'detail-name', detail.name); heading.id = 'detail-heading'; heading.tabIndex = -1;
    const scopedLabels = [...new Set((detail.definitions || []).filter(inYear).map((record) => record.label).filter(Boolean))];
    const visibleLabel = selectedYear() && scopedLabels.length === 1 ? scopedLabels[0] : detail.label;
    header.append(top, heading, el('p', 'detail-label', visibleLabel || 'Label not supplied'));
    const facts = el('dl', 'facts');
    const observed = (detail.observed_years || []).length;
    const hasCount = Number.isInteger(detail.null_count) && Number.isInteger(state.index.row_count);
    [['Years with values', observed ? yearsLabel(detail.observed_years) : 'No reported values'],
      ['Parquet storage', detail.storage_type || 'Not supplied'],
      ['Missing rows · full release', hasCount ? `${number(detail.null_count)} / ${number(state.index.row_count)}` : 'Not supplied'],
      ['Reported rows · full release', hasCount ? number(state.index.row_count - detail.null_count) : 'Not supplied']].forEach(([label, value]) => {
      const group = el('div'); addField(group, label, value); facts.append(group);
    });
    header.append(facts); container.append(header);
    const yearControl = el('label', 'detail-year-control', 'Values in year');
    const yearSelect = el('select'); yearSelect.id = 'detail-year-filter';
    const allYears = el('option', '', 'All years'); allYears.value = ''; yearSelect.append(allYears);
    (detail.observed_years || []).forEach((year) => { const option = el('option', '', year); option.value = year; yearSelect.append(option); });
    yearSelect.value = byId('year-filter').value;
    yearSelect.addEventListener('change', () => {
      byId('year-filter').value = yearSelect.value;
      applyFilters({ select: false });
      renderDetail(detail); updateLink(detail.name, true);
      byId('detail-year-filter').focus({ preventScroll: true });
      announce(selectedYear() ? `Showing ${detail.name} definitions and sources for ${selectedYear()}.` : `Showing ${detail.name} for all years.`);
    });
    yearControl.append(yearSelect); container.append(yearControl);
    if (selectedYear()) container.append(el('p', 'scope-note', `Definitions, codes and sources: ${selectedYear()}. Counts, measurement summary, gaps and Stata labels cover the full release.`));
    const tabs = el('div', 'detail-tabs'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', 'Variable reference sections');
    const panels = [];
    const sections = [['definition', 'Definition', renderDefinition], ['codes', 'Category labels', renderCodes], ['sources', 'Sources', renderSources], ['stata', 'Stata', renderStata]];
    const activate = (key, focus = false, navigate = false) => {
      state.tab = key;
      tabs.querySelectorAll('button').forEach((tab) => { const selected = tab.dataset.tab === key; tab.setAttribute('aria-selected', String(selected)); tab.tabIndex = selected ? 0 : -1; if (selected && focus) tab.focus(); });
      panels.forEach((panel) => { panel.hidden = panel.dataset.tab !== key; });
      if (navigate) updateLink(detail.name);
    };
    sections.forEach(([key, title, renderer]) => {
      const tab = button(title, '', () => activate(key, false, true)); tab.dataset.tab = key; tab.id = `tab-${key}`; tab.setAttribute('role', 'tab'); tab.setAttribute('aria-controls', `panel-${key}`);
      tabs.append(tab);
      const panel = el('section', 'tab-panel'); panel.id = `panel-${key}`; panel.dataset.tab = key; panel.dataset.printTitle = title; panel.setAttribute('role', 'tabpanel'); panel.setAttribute('aria-labelledby', tab.id); panel.tabIndex = 0;
      renderer(detail, panel); panels.push(panel);
    });
    tabs.addEventListener('keydown', (event) => {
      const keys = sections.map(([key]) => key); let position = keys.indexOf(state.tab);
      if (event.key === 'ArrowRight') position = (position + 1) % keys.length;
      else if (event.key === 'ArrowLeft') position = (position - 1 + keys.length) % keys.length;
      else if (event.key === 'Home') position = 0;
      else if (event.key === 'End') position = keys.length - 1;
      else return;
      event.preventDefault(); activate(keys[position], true, true);
    });
    container.append(tabs, ...panels);
    activate(state.tab);
  }
  function exportResults() {
    applyFilters();
    if (!state.matches.length) { announce('No matching variables to download.'); return; }
    const quote = (value) => {
      let text = String(value ?? '');
      // Prevent spreadsheet formula interpretation when opening the reference CSV.
      if (/^[=+@\t\r]/.test(text) || /^-\D/.test(text)) text = `'${text}`;
      return `"${text.replaceAll('"', '""')}"`;
    };
    const rows = [['variable', 'label', 'storage_type', 'stata_name', 'observed_years', 'sources', 'metadata_status', 'has_category_labels', 'has_metadata_gaps', 'missing_rows_full_release', 'release', 'selected_year_filter']];
    state.matches.forEach((item) => rows.push([item.name, item.label, item.storage_type, item.stata_name, yearsLabel(item.observed_years), (item.sources || []).join('; '), item.metadata_status, item.has_codes, item.has_issues, item.null_count, state.index.release, selectedYear() || 'all']));
    const csv = '\uFEFF' + rows.map((row) => row.map(quote).join(',')).join('\r\n') + '\r\n';
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }));
    const link = el('a'); link.href = url; link.download = `${state.index.release}-matching-variables.csv`; document.body.append(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    announce(`Exported metadata summaries for ${number(state.matches.length)} variables.`);
  }
  function readLink() {
    if (!state.index) return false;
    if (location.hash && !location.hash.includes('=')) return false;
    clearTimeout(searchTimer);
    const parameters = new URLSearchParams(location.hash.slice(1));
    const name = parameters.get('variable');
    // A deep link is self-contained, including when reached through browser Back.
    // Unrelated search and source filters must not hide its selected variable.
    byId('search').value = '';
    byId('source-filter').value = '';
    byId('code-filter').value = '';
    byId('gap-filter').checked = false;
    byId('year-filter').value = '';
    if (!name) {
      byId('explorer').classList.remove('detail-open');
      applyFilters();
      return true;
    }
    const item = state.index.variables.find((variable) => variable.name === name);
    if (!item) {
      state.selected = null; state.detail = null; state.token++;
      applyFilters({ select: false, sync: false, keepNotice: true });
      navigationNotice(`“${name}” is not a variable in this release. Check its spelling or switch to the other release.`);
      const empty = el('div', 'detail-placeholder');
      empty.append(el('h2', '', 'Variable not found'), el('p', '', 'Choose a variable from the results or clear the link to start again.'), button('Browse variables', '', resetFilters));
      byId('variable-detail').replaceChildren(empty);
      byId('variable-detail').setAttribute('aria-busy', 'false');
      byId('explorer').classList.remove('detail-open');
      return true;
    }
    const year = parameters.get('year');
    if (year && state.index.years.includes(Number(year)) && (item.observed_years || []).includes(Number(year))) byId('year-filter').value = year;
    const tab = tabNames.includes(parameters.get('tab')) ? parameters.get('tab') : 'definition';
    state.selected = name;
    applyFilters({ select: false, sync: false });
    selectVariable(name, { navigate: false, focus: false, openMobile: true, tab });
    if (year && !selectedYear()) navigationNotice(`Year “${year}” is not available in the reported-value coverage for ${name}. Showing all documented years.`);
    if (parameters.get('tab') && !tabNames.includes(parameters.get('tab'))) navigationNotice('That reference section does not exist. Showing the Definition tab.');
    updateLink(name, true);
    return true;
  }
  async function load() {
    byId('load-error').hidden = true;
    try {
      const index = await fetchJson('./codebook/index.json');
      if (index.schema_version !== 1 || !Array.isArray(index.variables) || !index.variables.length) throw new Error('This codebook format is not supported.');
      state.index = index;
      renderRelease();
      (index.years || []).forEach((year) => { const option = el('option', '', year); option.value = year; byId('year-filter').append(option); });
      (index.sources || []).forEach((source) => { const option = el('option', '', source); option.value = source; byId('source-filter').append(option); });
      controls.forEach((id) => { byId(id).disabled = false; });
      byId('variable-list').setAttribute('aria-busy', 'false');
      if (!readLink()) applyFilters({ sync: false });
      loadAliases();
    } catch (error) {
      const box = byId('load-error'); box.hidden = false;
      box.replaceChildren(el('p', '', 'The interactive codebook could not be loaded.'), el('p', '', `${error.message} The PDF and CSV downloads remain available above.`), button('Reload codebook', 'retry-button', () => location.reload()));
      byId('variable-list').setAttribute('aria-busy', 'false');
      byId('variable-list').replaceChildren(el('div', 'empty-results', 'Reference data unavailable. Please try reloading.'));
      byId('result-count').textContent = 'Unavailable';
    }
  }
  byId('search').addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => applyFilters(), 120); });
  ['year-filter', 'source-filter', 'code-filter', 'gap-filter'].forEach((id) => byId(id).addEventListener('change', () => applyFilters()));
  byId('reset-filters').addEventListener('click', resetFilters);
  byId('download-filtered').addEventListener('click', exportResults);
  byId('download-filtered').title = 'Download a CSV metadata summary for all matching variables';
  byId('show-more').addEventListener('click', () => {
    const list = byId('variable-list'), firstNew = state.limit, position = list.scrollTop;
    state.limit += 100; renderList();
    const next = list.children[firstNew];
    if (next) { next.focus({ preventScroll: true }); next.scrollIntoView({ block: 'nearest' }); }
    else { list.scrollTop = position; byId('search').focus({ preventScroll: true }); }
    announce(`Showing ${number(Math.min(state.limit, state.matches.length))} of ${number(state.matches.length)} variables.`);
  });
  window.addEventListener('hashchange', readLink);
  const downloadMenu = document.querySelector('.download-menu');
  document.addEventListener('pointerdown', (event) => {
    if (downloadMenu.open && !downloadMenu.contains(event.target)) downloadMenu.open = false;
  });
  let printState;
  window.addEventListener('beforeprint', () => {
    if (printState) return;
    const container = byId('variable-detail');
    const codeSearch = container.querySelector('.table-search input');
    printState = { codeSearch, query: codeSearch ? codeSearch.value : '', records: [] };
    if (codeSearch) { codeSearch.value = ''; codeSearch.dispatchEvent(new Event('input')); }
    container.querySelectorAll('.record-card, .source-evidence').forEach((record) => {
      printState.records.push([record, record.open]); record.open = true;
    });
  });
  window.addEventListener('afterprint', () => {
    if (!printState) return;
    const previous = printState; printState = null;
    previous.records.forEach(([record, wasOpen]) => { record.open = wasOpen; });
    if (previous.codeSearch) { previous.codeSearch.value = previous.query; previous.codeSearch.dispatchEvent(new Event('input')); }
  });
  const skipLink = document.querySelector('.skip-link');
  skipLink.addEventListener('click', (event) => {
    const search = byId('search');
    if (search.disabled) return;
    event.preventDefault();
    byId('explorer').classList.remove('detail-open');
    search.focus();
    search.scrollIntoView({ block: 'nearest' });
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && downloadMenu.open) {
      downloadMenu.open = false; downloadMenu.querySelector('summary').focus(); return;
    }
    if (event.key === '/' && !byId('search').disabled && !event.ctrlKey && !event.metaKey && !event.altKey && !['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement.tagName)) {
      event.preventDefault(); byId('explorer').classList.remove('detail-open'); byId('search').focus();
    }
    if (event.key === 'Escape' && isMobile() && byId('explorer').classList.contains('detail-open')) backToResults();
  });
  load();
})();
