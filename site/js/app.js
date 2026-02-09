/**
 * Main application orchestrator.
 * Wires up the router, lazy loader, state browser, and search.
 */
(function () {
  const app = document.getElementById('app');
  const breadcrumb = document.getElementById('breadcrumb');
  const searchInput = document.getElementById('search-input');
  const searchBtn = document.getElementById('search-btn');

  // All 50 states + DC with abbreviations for the home page grid
  const ALL_STATES = [
    { slug: 'alabama', abbr: 'AL', name: 'Alabama' },
    { slug: 'alaska', abbr: 'AK', name: 'Alaska' },
    { slug: 'arizona', abbr: 'AZ', name: 'Arizona' },
    { slug: 'arkansas', abbr: 'AR', name: 'Arkansas' },
    { slug: 'california', abbr: 'CA', name: 'California' },
    { slug: 'colorado', abbr: 'CO', name: 'Colorado' },
    { slug: 'connecticut', abbr: 'CT', name: 'Connecticut' },
    { slug: 'delaware', abbr: 'DE', name: 'Delaware' },
    { slug: 'district-of-columbia', abbr: 'DC', name: 'District of Columbia' },
    { slug: 'florida', abbr: 'FL', name: 'Florida' },
    { slug: 'georgia', abbr: 'GA', name: 'Georgia' },
    { slug: 'hawaii', abbr: 'HI', name: 'Hawaii' },
    { slug: 'idaho', abbr: 'ID', name: 'Idaho' },
    { slug: 'illinois', abbr: 'IL', name: 'Illinois' },
    { slug: 'indiana', abbr: 'IN', name: 'Indiana' },
    { slug: 'iowa', abbr: 'IA', name: 'Iowa' },
    { slug: 'kansas', abbr: 'KS', name: 'Kansas' },
    { slug: 'kentucky', abbr: 'KY', name: 'Kentucky' },
    { slug: 'louisiana', abbr: 'LA', name: 'Louisiana' },
    { slug: 'maine', abbr: 'ME', name: 'Maine' },
    { slug: 'maryland', abbr: 'MD', name: 'Maryland' },
    { slug: 'massachusetts', abbr: 'MA', name: 'Massachusetts' },
    { slug: 'michigan', abbr: 'MI', name: 'Michigan' },
    { slug: 'minnesota', abbr: 'MN', name: 'Minnesota' },
    { slug: 'mississippi', abbr: 'MS', name: 'Mississippi' },
    { slug: 'missouri', abbr: 'MO', name: 'Missouri' },
    { slug: 'montana', abbr: 'MT', name: 'Montana' },
    { slug: 'nebraska', abbr: 'NE', name: 'Nebraska' },
    { slug: 'nevada', abbr: 'NV', name: 'Nevada' },
    { slug: 'new-hampshire', abbr: 'NH', name: 'New Hampshire' },
    { slug: 'new-jersey', abbr: 'NJ', name: 'New Jersey' },
    { slug: 'new-mexico', abbr: 'NM', name: 'New Mexico' },
    { slug: 'new-york', abbr: 'NY', name: 'New York' },
    { slug: 'north-carolina', abbr: 'NC', name: 'North Carolina' },
    { slug: 'north-dakota', abbr: 'ND', name: 'North Dakota' },
    { slug: 'ohio', abbr: 'OH', name: 'Ohio' },
    { slug: 'oklahoma', abbr: 'OK', name: 'Oklahoma' },
    { slug: 'oregon', abbr: 'OR', name: 'Oregon' },
    { slug: 'pennsylvania', abbr: 'PA', name: 'Pennsylvania' },
    { slug: 'rhode-island', abbr: 'RI', name: 'Rhode Island' },
    { slug: 'south-carolina', abbr: 'SC', name: 'South Carolina' },
    { slug: 'south-dakota', abbr: 'SD', name: 'South Dakota' },
    { slug: 'tennessee', abbr: 'TN', name: 'Tennessee' },
    { slug: 'texas', abbr: 'TX', name: 'Texas' },
    { slug: 'utah', abbr: 'UT', name: 'Utah' },
    { slug: 'vermont', abbr: 'VT', name: 'Vermont' },
    { slug: 'virginia', abbr: 'VA', name: 'Virginia' },
    { slug: 'washington', abbr: 'WA', name: 'Washington' },
    { slug: 'west-virginia', abbr: 'WV', name: 'West Virginia' },
    { slug: 'wisconsin', abbr: 'WI', name: 'Wisconsin' },
    { slug: 'wyoming', abbr: 'WY', name: 'Wyoming' },
  ];

  // --- Search bar ---
  function doSearch() {
    const q = searchInput.value.trim();
    if (q) Router.navigate(`/search?q=${encodeURIComponent(q)}`);
  }
  searchBtn.addEventListener('click', doSearch);
  searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') doSearch();
  });

  // --- Breadcrumb helper ---
  function setBreadcrumb(crumbs) {
    if (!crumbs || crumbs.length === 0) {
      breadcrumb.innerHTML = '';
      return;
    }
    breadcrumb.innerHTML = crumbs.map((c, i) => {
      if (i === crumbs.length - 1) return `<span>${esc(c.label)}</span>`;
      return `<a href="${c.href}">${esc(c.label)}</a>`;
    }).join('<span class="separator">/</span>');
  }

  function esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function titleCase(slug) {
    return slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  // --- Routes ---

  // Home page
  Router.addRoute('/', () => {
    setBreadcrumb([]);
    let html = `<div class="home-page">`;
    html += `<h1>US Statutes</h1>`;
    html += `<p class="subtitle">Browse the statutes of all 50 US states and the District of Columbia</p>`;
    html += `<div id="us-map-container"></div>`;
    html += `<h2 style="margin-top:1.5rem;margin-bottom:0.75rem;font-size:1.1rem;color:var(--color-text-secondary)">All Jurisdictions</h2>`;
    html += `<div class="state-grid">`;

    for (const s of ALL_STATES) {
      html += `<a class="state-card" href="#/states/${s.slug}">
        <span class="abbr">${s.abbr}</span>
        <span class="name">${s.name}</span>
      </a>`;
    }

    html += `</div></div>`;
    app.innerHTML = html;

    // Load and wire up the SVG map
    const mapContainer = document.getElementById('us-map-container');
    if (mapContainer) {
      fetch('assets/us-map.svg')
        .then(r => r.ok ? r.text() : '')
        .then(svg => {
          if (!svg) return;
          mapContainer.innerHTML = svg;
          mapContainer.querySelectorAll('.state').forEach(el => {
            el.addEventListener('click', () => {
              const slug = el.id;
              if (slug) Router.navigate(`/states/${slug}`);
            });
          });
        })
        .catch(() => {});
    }
  });

  // State TOC page
  Router.addRoute('/states/:state', async (params) => {
    const state = params.state;
    app.innerHTML = '<div class="loading">Loading state data...</div>';
    setBreadcrumb([
      { label: 'Home', href: '#/' },
      { label: titleCase(state) },
    ]);

    try {
      const [manifest, toc] = await Promise.all([
        LazyLoader.loadManifest(state),
        LazyLoader.loadTOC(state),
      ]);
      StateBrowser.renderStatePage(manifest, toc, app);
    } catch (err) {
      app.innerHTML = `<div class="error">
        <h2>State not found</h2>
        <p>Could not load data for "${esc(titleCase(state))}". This state may not have been ingested yet.</p>
        <p><a href="#/">Back to home</a></p>
      </div>`;
    }
  });

  // Chapter content page
  Router.addRoute('/states/:state/*path', async (params) => {
    const state = params.state;
    const path = params.path;
    app.innerHTML = '<div class="loading">Loading chapter...</div>';
    setBreadcrumb([
      { label: 'Home', href: '#/' },
      { label: titleCase(state), href: `#/states/${state}` },
      { label: path },
    ]);

    try {
      const [manifest, chapter] = await Promise.all([
        LazyLoader.loadManifest(state),
        LazyLoader.loadChapter(state, path),
      ]);
      StateBrowser.renderChapterPage(chapter, manifest, app);
    } catch (err) {
      app.innerHTML = `<div class="error">
        <h2>Chapter not found</h2>
        <p>Could not load "${esc(path)}" for ${esc(titleCase(state))}.</p>
        <p><a href="#/states/${esc(state)}">Back to ${esc(titleCase(state))}</a></p>
      </div>`;
    }
  });

  // Search page
  Router.addRoute('/search', async (params) => {
    const query = params.q || '';
    const stateFilter = params.state || '';
    searchInput.value = query;

    app.innerHTML = '<div class="loading">Searching...</div>';
    setBreadcrumb([
      { label: 'Home', href: '#/' },
      { label: 'Search' },
    ]);

    if (!query) {
      app.innerHTML = '<div class="search-page"><h1>Search</h1><p>Enter a search term above.</p></div>';
      return;
    }

    try {
      const results = await Search.search(query, stateFilter);
      Search.renderResults(results, app, query);
    } catch (err) {
      app.innerHTML = `<div class="error"><h2>Search error</h2><p>${esc(err.message)}</p></div>`;
    }
  });

  // 404
  Router.setNotFound((path) => {
    setBreadcrumb([{ label: 'Home', href: '#/' }, { label: 'Not Found' }]);
    app.innerHTML = `<div class="error"><h2>Page not found</h2><p>No page at "${esc(path)}".</p><p><a href="#/">Go home</a></p></div>`;
  });

  // Start
  Router.init();
})();
