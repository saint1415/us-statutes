/**
 * Hash-based SPA router.
 *
 * Routes:
 *   #/                           -> home
 *   #/states/{state}             -> state TOC
 *   #/states/{state}/{path...}   -> chapter content
 *   #/search?q=...&state=...     -> search results
 */
const Router = (() => {
  const routes = [];
  let notFoundHandler = null;

  function addRoute(pattern, handler) {
    // Convert route pattern to regex
    // e.g., "/states/:state" -> /^\/states\/([^/?#]+)$/
    // e.g., "/states/:state/*path" -> /^\/states\/([^/?#]+)\/(.+)$/
    const paramNames = [];
    let regexStr = pattern.replace(/:(\w+)/g, (_, name) => {
      paramNames.push(name);
      return '([^/?#]+)';
    }).replace(/\*(\w+)/g, (_, name) => {
      paramNames.push(name);
      return '(.+)';
    });
    const regex = new RegExp('^' + regexStr + '$');
    routes.push({ regex, paramNames, handler });
  }

  function setNotFound(handler) {
    notFoundHandler = handler;
  }

  function parseHash() {
    const hash = window.location.hash.slice(1) || '/';
    const [pathPart, queryPart] = hash.split('?');
    const params = {};
    if (queryPart) {
      new URLSearchParams(queryPart).forEach((v, k) => { params[k] = v; });
    }
    return { path: pathPart || '/', query: params };
  }

  function resolve() {
    const { path, query } = parseHash();

    for (const route of routes) {
      const match = path.match(route.regex);
      if (match) {
        const params = { ...query };
        route.paramNames.forEach((name, i) => {
          params[name] = decodeURIComponent(match[i + 1]);
        });
        route.handler(params);
        return;
      }
    }

    if (notFoundHandler) {
      notFoundHandler(path);
    }
  }

  function navigate(path) {
    window.location.hash = '#' + path;
  }

  function init() {
    window.addEventListener('hashchange', resolve);
    resolve();
  }

  return { addRoute, setNotFound, navigate, init, parseHash };
})();
