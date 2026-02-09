/**
 * Search integration - uses Pagefind when available, falls back to basic search.
 */
const Search = (() => {
  let pagefind = null;
  let initialized = false;

  async function init() {
    if (initialized) return;
    initialized = true;

    try {
      // Try to load Pagefind (built at deploy time)
      pagefind = await import('/pagefind/pagefind.js');
      await pagefind.init();
    } catch {
      // Pagefind not available - search will use fallback
      pagefind = null;
    }
  }

  async function search(query, stateFilter) {
    await init();

    if (pagefind) {
      return _pagefindSearch(query, stateFilter);
    }
    return _fallbackSearch(query, stateFilter);
  }

  async function _pagefindSearch(query, stateFilter) {
    const filters = {};
    if (stateFilter) {
      filters.state = stateFilter;
    }

    const results = await pagefind.search(query, { filters });
    const items = [];

    // Load first 20 results
    const slice = results.results.slice(0, 20);
    for (const result of slice) {
      const data = await result.data();
      items.push({
        title: data.meta?.title || 'Untitled',
        url: data.url || '#',
        excerpt: data.excerpt || '',
        state: data.filters?.state || '',
      });
    }

    return { total: results.results.length, items };
  }

  async function _fallbackSearch(query, stateFilter) {
    // Basic fallback: search through loaded TOC data
    // This won't search full text, only headings
    const results = { total: 0, items: [] };

    try {
      const index = await LazyLoader.loadMasterIndex();
      const queryLower = query.toLowerCase();

      for (const state of index.states) {
        if (stateFilter && state.state !== stateFilter) continue;

        try {
          const toc = await LazyLoader.loadTOC(state.state);
          _searchTOC(toc.children, state, queryLower, results.items);
        } catch {
          // Skip states without TOC data
        }
      }

      results.total = results.items.length;
    } catch {
      // No index available
    }

    return results;
  }

  function _searchTOC(children, state, query, results) {
    if (!children) return;
    for (const node of children) {
      const headingMatch = node.heading?.toLowerCase().includes(query);
      const numberMatch = node.number?.toLowerCase().includes(query);
      if (headingMatch || numberMatch) {
        results.push({
          title: `\u00a7 ${node.number} ${node.heading}`,
          url: `#/states/${state.state}`,
          excerpt: `${state.code_name} - ${node.heading}`,
          state: state.state,
        });
      }
      if (node.children) {
        _searchTOC(node.children, state, query, results);
      }
    }
  }

  function renderResults(searchData, container, query) {
    let html = `<div class="search-page">`;
    html += `<h1>Search results for "${_esc(query)}"</h1>`;
    html += `<p>${searchData.total} result(s) found</p>`;

    if (searchData.items.length === 0) {
      html += `<p>No results found. Try a different search term.</p>`;
    } else {
      for (const item of searchData.items) {
        html += `
          <div class="search-result">
            <div class="search-result-title"><a href="${item.url}">${item.title || 'Untitled'}</a></div>
            ${item.state ? `<div class="search-result-path">${_esc(_titleCase(item.state))}</div>` : ''}
            <div class="search-result-excerpt">${item.excerpt}</div>
          </div>
        `;
      }
    }

    html += `</div>`;
    container.innerHTML = html;
  }

  function _esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function _titleCase(slug) {
    return slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  return { init, search, renderResults };
})();
