/**
 * Lazy-loads JSON data files on demand with caching.
 * Handles both main-branch (TOC/manifest) and data-branch (content) files.
 */
const LazyLoader = (() => {
  const cache = new Map();

  // Base URL for data files - auto-detected from hosting context
  function getBaseUrl() {
    // For GitHub Pages, data files are served from the same origin
    // after the deploy workflow merges main + data branches
    const base = document.querySelector('base')?.href || '';
    if (base) return base.replace(/\/$/, '');
    // Detect GitHub Pages path
    const path = window.location.pathname;
    const match = path.match(/^\/([^/]+)\//);
    if (match) return `/${match[1]}`;
    return '';
  }

  const BASE = getBaseUrl();

  async function loadJSON(path) {
    if (cache.has(path)) {
      return cache.get(path);
    }

    const url = `${BASE}/data/${path}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to load ${url}: ${response.status}`);
    }
    const data = await response.json();
    cache.set(path, data);
    return data;
  }

  async function loadMasterIndex() {
    return loadJSON('index.json');
  }

  async function loadManifest(state) {
    return loadJSON(`states/${state}/manifest.json`);
  }

  async function loadTOC(state) {
    return loadJSON(`states/${state}/toc.json`);
  }

  async function loadChapter(state, chapterPath) {
    return loadJSON(`states/${state}/content/${chapterPath}.json`);
  }

  function clearCache() {
    cache.clear();
  }

  return { loadMasterIndex, loadManifest, loadTOC, loadChapter, clearCache };
})();
