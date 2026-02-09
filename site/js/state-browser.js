/**
 * State browser: renders TOC trees and chapter content.
 */
const StateBrowser = (() => {

  function renderStatePage(manifest, toc, container) {
    const stateName = manifest.state_abbr
      ? `${_titleCase(manifest.state)} (${manifest.state_abbr})`
      : _titleCase(manifest.state);

    container.innerHTML = `
      <div class="state-page">
        <h1>${_esc(stateName)}</h1>
        <div class="code-name">${_esc(manifest.code_name)}</div>
        <div class="state-meta">
          Source: ${_esc(manifest.source)} |
          Updated: ${_esc(_formatDate(manifest.last_updated))} |
          ${manifest.stats.sections?.toLocaleString() || '?'} sections
        </div>
        <ul class="toc-tree" id="toc-root"></ul>
      </div>
    `;

    const tocRoot = container.querySelector('#toc-root');
    _renderTOCLevel(toc.children, tocRoot, manifest.state, []);
  }

  function _renderTOCLevel(children, parentEl, state, pathParts) {
    if (!children || children.length === 0) return;

    for (const node of children) {
      const li = document.createElement('li');
      li.className = 'toc-item';

      const hasChildren = node.children && node.children.length > 0;
      const isSection = !hasChildren && !node.section_count;

      if (isSection) {
        // Leaf section - render as link
        const a = document.createElement('a');
        a.className = 'toc-section-link';
        a.innerHTML = `<span class="toc-number">\u00a7 ${_esc(node.number)}</span> ${_esc(node.heading)}`;
        // Section links point to the chapter that contains them
        a.href = `#/states/${state}/${pathParts.join('/')}`;
        li.appendChild(a);
      } else {
        // Branch node - render as expandable
        const toggle = document.createElement('div');
        toggle.className = 'toc-toggle';

        const arrow = document.createElement('span');
        arrow.className = 'toc-arrow';
        arrow.textContent = '\u25b6';

        const number = document.createElement('span');
        number.className = 'toc-number';
        number.textContent = node.number;

        const heading = document.createElement('span');
        heading.className = 'toc-heading';
        heading.textContent = node.heading;

        toggle.appendChild(arrow);
        toggle.appendChild(number);
        toggle.appendChild(heading);

        if (node.section_count) {
          const count = document.createElement('span');
          count.className = 'toc-count';
          count.textContent = `(${node.section_count} sections)`;
          toggle.appendChild(count);

          // Chapters with section_count are navigable
          toggle.style.cursor = 'pointer';
          const chapterPath = [...pathParts, node.id].join('/');
          toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            Router.navigate(`/states/${state}/${chapterPath}`);
          });
        }

        li.appendChild(toggle);

        if (hasChildren) {
          const childUl = document.createElement('ul');
          childUl.className = 'toc-children';
          _renderTOCLevel(node.children, childUl, state, [...pathParts, node.id]);
          li.appendChild(childUl);

          toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            arrow.classList.toggle('open');
            childUl.classList.toggle('open');
          });
        }
      }

      parentEl.appendChild(li);
    }
  }

  function renderChapterPage(chapterData, manifest, container) {
    const stateName = _titleCase(chapterData.state);

    let html = `<div class="chapter-page">`;
    html += `<h1>${_esc(stateName)} - ${_esc(chapterData.path)}</h1>`;

    for (const section of chapterData.sections) {
      html += `
        <div class="section-block" id="${_esc(section.id)}">
          <div class="section-header">
            <span class="section-number">\u00a7 ${_esc(section.number)}</span>
            <span class="section-heading">${_esc(section.heading)}</span>
            <a class="section-permalink" href="#/states/${_esc(chapterData.state)}/${_esc(chapterData.path)}#${_esc(section.id)}" title="Permalink">#</a>
          </div>
          <div class="section-text">${_esc(section.text)}</div>
          ${section.history ? `<div class="section-history">${_esc(section.history)}</div>` : ''}
          ${section.source_url ? `<div class="section-source"><a href="${_esc(section.source_url)}" target="_blank" rel="noopener">View original source</a></div>` : ''}
        </div>
      `;
    }

    html += `</div>`;
    container.innerHTML = html;

    // Scroll to section if hash fragment present
    const hash = window.location.hash;
    const sectionHash = hash.split('#').pop();
    if (sectionHash && sectionHash.startsWith('section-')) {
      const el = document.getElementById(sectionHash);
      if (el) el.scrollIntoView({ behavior: 'smooth' });
    }
  }

  function _titleCase(slug) {
    return slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  function _esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function _formatDate(isoStr) {
    if (!isoStr) return 'Unknown';
    try {
      return new Date(isoStr).toLocaleDateString('en-US', {
        year: 'numeric', month: 'short', day: 'numeric'
      });
    } catch {
      return isoStr;
    }
  }

  return { renderStatePage, renderChapterPage };
})();
