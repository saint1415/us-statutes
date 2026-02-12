/**
 * Lazy-loads JSON data files on demand with caching.
 * TOC/manifest files are served from GitHub Pages (same origin).
 * Content files are fetched from the raw data branch on GitHub
 * (too large for the 1GB Pages deployment limit).
 */
const LazyLoader = (() => {
  const cache = new Map();

  // GitHub raw content URL for the data branch (content files)
  const CONTENT_BASE = 'https://raw.githubusercontent.com/saint1415/us-statutes/data';

  // Base URL for site-local data files (TOC, manifests, index)
  function getBaseUrl() {
    const base = document.querySelector('base')?.href || '';
    if (base) return base.replace(/\/$/, '');
    const path = window.location.pathname;
    const match = path.match(/^\/([^/]+)\//);
    if (match) return `/${match[1]}`;
    return '';
  }

  const BASE = getBaseUrl();

  async function loadJSON(url) {
    if (cache.has(url)) {
      return cache.get(url);
    }

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to load ${url}: ${response.status}`);
    }
    const data = await response.json();
    cache.set(url, data);
    return data;
  }

  async function loadMasterIndex() {
    return loadJSON(`${BASE}/data/index.json`);
  }

  async function loadManifest(state) {
    return loadJSON(`${BASE}/data/states/${state}/manifest.json`);
  }

  async function loadTOC(state) {
    return loadJSON(`${BASE}/data/states/${state}/toc.json`);
  }

  async function loadChapter(state, chapterPath) {
    // Content files are fetched from the raw data branch
    return loadJSON(`${CONTENT_BASE}/data/states/${state}/content/${chapterPath}.json`);
  }

  function clearCache() {
    cache.clear();
  }

  return { loadMasterIndex, loadManifest, loadTOC, loadChapter, clearCache };
})();
