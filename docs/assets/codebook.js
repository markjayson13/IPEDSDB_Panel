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
  const state = { index: null, matches: [], selected: null, detail: null, limit: 100, tab: 'definition', token: 0 };
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
    const response = await fetch(url);
    if (!response.ok) throw new Error(`The codebook file could not be loaded (HTTP ${response.status}).`);
    return response.json();
  }
  function downloadPath(name) {
    if (typeof name !== 'string' || !/^[a-zA-Z0-9._-]+$/.test(name)) return null;
    return `./codebook/${name}`;
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
          if (filename.endsWith('.gz') && !link.textContent.includes('(gzip)')) link.append(document.createTextNode(' (gzip)'));
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
      const link = el('a', 'text-link', 'Download the original-to-canonical column crosswalk (CSV)');
      link.href = `./codebook/${index.downloads.column_crosswalk}`;
      link.download = '';
      content.append(link);
    }
    const gapVariables = index.variables.filter((item) => item.has_issues);
    if (gapVariables.length) {
      const notice = el('div', 'gap-callout');
      notice.append(el('strong', '', `${number(gapVariables.length)} variables have documented metadata gaps.`));
      notice.append(el('p', '', 'Use the Metadata gaps filter to inspect them. An undocumented meaning stays undocumented; a label from a different year is not evidence for that observation.'));
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
      fragment.append(row);
    });
    list.append(fragment);
  }
  function applyFilters() {
    if (!state.index) return;
    const query = byId('search').value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
    const year = selectedYear();
    const source = byId('source-filter').value;
    const codes = byId('code-filter').value;
    const gaps = byId('gap-filter').checked;
    state.matches = state.index.variables.filter((item) => {
      const text = `${item.name} ${item.label} ${(item.sources || []).join(' ')}`.toLocaleLowerCase();
      return query.every((word) => text.includes(word)) && (!year || (item.observed_years || []).includes(year)) &&
        (!source || (item.sources || []).includes(source)) && (!codes || (codes === 'coded' ? item.has_codes : !item.has_codes)) && (!gaps || item.has_issues);
    });
    state.limit = 100;
    renderList();
    byId('variable-list').scrollTop = 0;
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
      empty.append(el('h2', '', 'Broaden your search.'), el('p', '', 'No variables match all the active filters. Clear a filter to continue exploring.'));
      byId('variable-detail').replaceChildren(empty);
    }
  }
  function resetFilters() {
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
    parameters.set('variable', name);
    if (selectedYear()) parameters.set('year', String(selectedYear()));
    const hash = `#${parameters.toString()}`;
    if (location.hash !== hash) history[replace ? 'replaceState' : 'pushState'](null, '', hash);
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
    const changed = state.selected !== name;
    state.selected = name;
    state.detail = null;
    if (changed) state.tab = 'definition';
    const token = ++state.token;
    byId('variable-list').querySelectorAll('.variable-item').forEach((row) => row.setAttribute('aria-current', String(row.dataset.name === name)));
    if (options.navigate) updateLink(name);
    if (options.openMobile) byId('explorer').classList.add('detail-open');
    const detail = byId('variable-detail');
    detail.setAttribute('aria-busy', 'true');
    const loading = el('div', 'detail-placeholder');
    loading.append(el('h2', '', name), el('p', '', 'Loading definitions and source records…'));
    detail.replaceChildren(loading);
    try {
      if (!/^variables-\d+\.json$/.test(item.detail_file)) throw new Error('The variable reference file is invalid.');
      if (!cache.has(item.detail_file)) cache.set(item.detail_file, fetchJson(`./codebook/${item.detail_file}`).catch((error) => { cache.delete(item.detail_file); throw error; }));
      const shard = await cache.get(item.detail_file);
      if (token !== state.token) return;
      if (!Object.prototype.hasOwnProperty.call(shard, name)) throw new Error('The variable detail is missing from this release.');
      state.detail = shard[name];
      renderDetail(state.detail);
      detail.scrollTop = 0;
      if (options.focus) byId('detail-heading').focus({ preventScroll: true });
      if (options.openMobile && isMobile()) detail.scrollIntoView({ block: 'start' });
      announce(`Showing ${name}. ${state.detail.label || ''}`);
    } catch (error) {
      if (token !== state.token) return;
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
    panel.append(el('h3', '', 'Definitions by year'), el('p', 'section-intro', 'Each definition applies only to its listed years. “Not supplied” means the source does not provide that text.'));
    const definitions = (detail.definitions || []).filter(inYear);
    if (definitions.length) {
      definitions.forEach((definition) => {
        const block = el('section', 'definition-block');
        block.append(el('span', 'year-tag', yearsLabel(selectedYear() ? [selectedYear()] : definition.years)));
        block.append(el('h4', '', definition.label || 'Label not supplied'));
        block.append(el('p', definition.description ? 'data-description' : 'blank-value', definition.description || 'Description not supplied'));
        panel.append(block);
      });
    } else panel.append(el('p', 'blank-value', detail.description || 'No definition is supplied for this year selection.'));
    const measurement = el('section', 'subsection');
    measurement.append(el('h3', '', 'Measurement context'), el('p', 'section-intro', 'Unknown fields are not inferred from a variable name. Consult source reference periods before comparing years.'));
    const semantics = detail.semantic_metadata || {};
    const grid = el('dl', 'semantic-grid');
    ['units', 'currency', 'price_basis', 'reference_period'].forEach((key) => {
      const group = semantics[key];
      const item = el('div');
      item.append(el('dt', '', readable(key)));
      let text = 'Not supplied';
      if (group && typeof group === 'object') {
        text = group.value !== null && group.value !== undefined && group.value !== '' ? values(group.value) :
          (group.values && group.values.length ? group.values.map(values).join('; ') : 'Unknown in source metadata');
        if (group.status && group.status !== 'complete') text += ` · ${readable(group.status)}`;
      } else if (group) text = values(group);
      item.append(el('dd', '', text));
      grid.append(item);
    });
    measurement.append(grid);
    if (Object.keys(semantics).length) measurement.append(makeTechnical('Inspect measurement metadata', semantics));
    panel.append(measurement);
  }
  function renderCodes(detail, panel) {
    panel.append(el('h3', '', 'Category meanings'), el('p', 'section-intro', 'These are original panel codes, with meanings restricted to the documented years. Stata may represent string categories with different numeric codes; see the Stata tab.'));
    const issue = renderIssues(detail);
    if (issue) panel.append(issue);
    const records = (detail.codes || []).filter(inYear);
    if (!records.length) {
      panel.append(el('p', 'blank-value', 'No category labels are documented for this year selection. This does not establish that every value is continuous or uncoded.'));
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
      source.append(el('h3', '', 'Codebook evidence'), el('p', '', 'Some meanings use verified, same-year source dictionaries. Original records and the evidence for each addition are retained.'));
      const scoped = detail.code_provenance.filter(inYear);
      source.append(makeTechnical('Inspect code-label source evidence', scoped));
      if (detail.original_codes && detail.original_codes.length) source.append(makeTechnical('Preserved original code-label records', detail.original_codes.filter(inYear)));
      panel.append(source);
    }
  }
  function renderSources(detail, panel) {
    panel.append(el('h3', '', 'Source records'), el('p', 'section-intro', 'The physical table identifies the resolved source. Where a documented correction applies, the original dictionary table remains recorded separately.'));
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
        ['Source component', record.source_file], ['Physical table', record.table], ['Original table', record.original_table],
        ['Original variable', record.varname], ['Variable number', record.varnumber],
        ['Source title', record.title || definition.label], ['Reference period', record.reference_period], ['Imputation variable', record.imputationvar]
      ];
      entries.forEach(([name, value]) => addField(fields, name, value));
      const excluded = new Set(['years', 'source_file', 'table', 'original_table', 'varname', 'varnumber', 'title', 'description', 'definition_index', 'reference_period', 'imputationvar', 'correction_id', 'correction_reason']);
      Object.entries(record).forEach(([key, value]) => { if (!excluded.has(key) && value !== null && value !== '') addField(fields, readable(key), value); });
      card.append(fields);
      if (record.correction_id || record.correction_reason) {
        const note = el('div', 'correction-note');
        note.append(el('strong', '', record.correction_id || 'Documented correction'), el('p', '', record.correction_reason || 'See the preserved verification evidence in this record.'));
        card.append(note);
      }
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
      panel.append(table(['Original panel code', 'Stata code', 'Stata value label'], map.map((item) => [item.source_code, item.export_code, item.label]), ['code-cell', 'code-cell', '']));
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
    const header = el('header');
    const top = el('div', 'detail-header-top');
    const badges = el('div', 'detail-kicker');
    (detail.sources || []).forEach((source) => badges.append(el('span', 'pill', source)));
    if (!detail.sources || !detail.sources.length) badges.append(el('span', 'pill', 'Panel variable'));
    if (detail.has_issues) badges.append(el('span', 'pill warning', 'Metadata gaps'));
    const actions = el('div', 'detail-actions');
    const copy = button('Copy link ↗', '', async () => {
      updateLink(detail.name, true);
      try { await navigator.clipboard.writeText(location.href); copy.textContent = 'Link copied'; announce('Variable link copied.'); }
      catch (_) {
        const field = el('input'); field.value = location.href; field.readOnly = true; field.setAttribute('aria-label', 'Variable link to copy');
        actions.append(field); field.focus(); field.select(); copy.textContent = 'Select and copy';
      }
    });
    actions.append(copy, button('Print variable', '', () => {
      const codeSearch = container.querySelector('.table-search input');
      const previousCodeSearch = codeSearch ? codeSearch.value : '';
      if (codeSearch) { codeSearch.value = ''; codeSearch.dispatchEvent(new Event('input')); }
      const restore = [];
      container.querySelectorAll('.record-card').forEach((record) => { restore.push([record, record.open]); record.open = true; });
      window.print();
      restore.forEach(([record, wasOpen]) => { record.open = wasOpen; });
      if (codeSearch) { codeSearch.value = previousCodeSearch; codeSearch.dispatchEvent(new Event('input')); }
    }));
    top.append(badges, actions);
    const heading = el('h2', 'detail-name', detail.name); heading.id = 'detail-heading'; heading.tabIndex = -1;
    header.append(top, heading, el('p', 'detail-label', detail.label || 'Label not supplied'));
    const facts = el('dl', 'facts');
    const observed = (detail.observed_years || []).length;
    [['Observed years', yearsLabel(detail.observed_years)], ['Parquet storage', detail.storage_type || 'Not supplied'], ['Missing observations', detail.null_count === null || detail.null_count === undefined ? 'Not supplied' : number(detail.null_count)], ['Coverage', `${observed} year${observed === 1 ? '' : 's'} with values`]].forEach(([label, value]) => {
      const group = el('div'); addField(group, label, value); facts.append(group);
    });
    header.append(facts); container.append(header);
    if (selectedYear()) container.append(el('p', 'scope-note', `Viewing definitions, category meanings, and sources for ${selectedYear()}. Variable coverage, gaps, and Stata export labels describe the full release.`));
    const tabs = el('div', 'detail-tabs'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', 'Variable reference sections');
    const panels = [];
    const sections = [['definition', 'Definition', renderDefinition], ['codes', 'Category labels', renderCodes], ['sources', 'Sources', renderSources], ['stata', 'Stata', renderStata]];
    const activate = (key, focus = false) => {
      state.tab = key;
      tabs.querySelectorAll('button').forEach((tab) => { const selected = tab.dataset.tab === key; tab.setAttribute('aria-selected', String(selected)); tab.tabIndex = selected ? 0 : -1; if (selected && focus) tab.focus(); });
      panels.forEach((panel) => { panel.hidden = panel.dataset.tab !== key; });
    };
    sections.forEach(([key, title, renderer]) => {
      const tab = button(title, '', () => activate(key)); tab.dataset.tab = key; tab.id = `tab-${key}`; tab.setAttribute('role', 'tab'); tab.setAttribute('aria-controls', `panel-${key}`);
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
      event.preventDefault(); activate(keys[position], true);
    });
    container.append(tabs, ...panels);
    activate(state.tab);
  }
  function exportResults() {
    const quote = (value) => {
      let text = String(value ?? '');
      // Prevent spreadsheet formula interpretation when opening the reference CSV.
      if (/^[=+@\t\r]/.test(text) || /^-\D/.test(text)) text = `'${text}`;
      return `"${text.replaceAll('"', '""')}"`;
    };
    const rows = [['variable', 'label', 'storage_type', 'stata_name', 'observed_years', 'sources', 'metadata_status', 'has_category_labels', 'has_metadata_gaps', 'missing_observations', 'release', 'selected_year_filter']];
    state.matches.forEach((item) => rows.push([item.name, item.label, item.storage_type, item.stata_name, yearsLabel(item.observed_years), (item.sources || []).join('; '), item.metadata_status, item.has_codes, item.has_issues, item.null_count, state.index.release, selectedYear() || 'all']));
    const csv = '\uFEFF' + rows.map((row) => row.map(quote).join(',')).join('\r\n') + '\r\n';
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }));
    const link = el('a'); link.href = url; link.download = 'ipeds-codebook-search-results.csv'; document.body.append(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    announce(`Exported metadata summaries for ${number(state.matches.length)} variables.`);
  }
  function readLink() {
    if (!state.index) return false;
    const parameters = new URLSearchParams(location.hash.slice(1));
    const name = parameters.get('variable');
    if (!name) { if (isMobile()) byId('explorer').classList.remove('detail-open'); return false; }
    const item = state.index.variables.find((variable) => variable.name === name);
    if (!item) { announce(`Variable ${name} is not in this release.`); return false; }
    // A deep link is self-contained, including when reached through browser Back.
    // Unrelated search and source filters must not hide its selected variable.
    byId('search').value = '';
    byId('source-filter').value = '';
    byId('code-filter').value = '';
    byId('gap-filter').checked = false;
    const year = parameters.get('year');
    if (year && state.index.years.includes(Number(year)) && (item.observed_years || []).includes(Number(year))) byId('year-filter').value = year;
    else byId('year-filter').value = '';
    applyFilters();
    selectVariable(name, { navigate: false, focus: false, openMobile: true });
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
      if (!readLink()) applyFilters();
    } catch (error) {
      const box = byId('load-error'); box.hidden = false;
      box.replaceChildren(el('p', '', 'The interactive codebook could not be loaded.'), el('p', '', `${error.message} The PDF and CSV downloads remain available above.`), button('Reload codebook', 'retry-button', () => location.reload()));
      byId('variable-list').setAttribute('aria-busy', 'false');
      byId('variable-list').replaceChildren(el('div', 'empty-results', 'Reference data unavailable. Please try reloading.'));
      byId('result-count').textContent = 'Unavailable';
    }
  }
  byId('search').addEventListener('input', applyFilters);
  ['year-filter', 'source-filter', 'code-filter', 'gap-filter'].forEach((id) => byId(id).addEventListener('change', applyFilters));
  byId('reset-filters').addEventListener('click', resetFilters);
  byId('download-filtered').addEventListener('click', exportResults);
  byId('download-filtered').title = 'Download a CSV metadata summary for all matching variables';
  byId('show-more').addEventListener('click', () => { const position = byId('variable-list').scrollTop; state.limit += 100; renderList(); byId('variable-list').scrollTop = position; });
  window.addEventListener('hashchange', readLink);
  document.addEventListener('keydown', (event) => {
    if (event.key === '/' && !event.ctrlKey && !event.metaKey && !event.altKey && !['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement.tagName)) {
      event.preventDefault(); byId('explorer').classList.remove('detail-open'); byId('search').focus();
    }
    if (event.key === 'Escape' && isMobile() && byId('explorer').classList.contains('detail-open')) backToResults();
  });
  load();
})();
