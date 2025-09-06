/* static/i18n.js
 * Loader simple de traducciones:
 * - Atributo data-i18n="clave" en elementos que deben traducirse.
 * - Usa localStorage 'pref.lang' (gestionado por prefs.js).
 * - Escucha 'app:lang-changed' para volver a aplicar.
 * - Expone window.I18N.t(key) para que el JS pueda pedir strings.
 */
(function () {
  "use strict";

  const LANG_KEY = "pref.lang";
  const CACHE = new Map(); // lang -> dict
  let currentLang = null;
  let dict = {};

  async function loadLang(lang) {
    if (CACHE.has(lang)) {
      dict = CACHE.get(lang);
      currentLang = lang;
      return dict;
    }
    const url = `/elective_office/static/i18n/${lang}.json`;
    const res = await fetch(url, { credentials: "same-origin" });
    if (!res.ok) throw new Error(`i18n: HTTP ${res.status} for ${url}`);
    const data = await res.json();
    CACHE.set(lang, data);
    dict = data;
    currentLang = lang;
    return dict;
  }

  // Híbrida: soporta claves planas con punto Y anidadas
  function t(path) {
    if (!dict) return path;

    // 1) Match exacto (para claves planas como "app.processed_results")
    if (Object.prototype.hasOwnProperty.call(dict, path)) {
      const val = dict[path];
      return (val === undefined || val === null) ? path : val;
    }

    // 2) Recorrer por puntos (para claves anidadas como "btn.rename")
    const parts = String(path).split(".");
    let node = dict;
    for (const p of parts) {
      if (node && Object.prototype.hasOwnProperty.call(node, p)) {
        node = node[p];
      } else {
        return path; // fallback: deja la clave si no existe
      }
    }
    return (node === undefined || node === null) ? path : node;
  }

  function applyTranslations(root = document) {
    root.querySelectorAll("[data-i18n]").forEach((el) => {
      const key = el.getAttribute("data-i18n");
      if (!key) return;
      const val = t(key);
      if (val === key) return; // si no hay traducción, conservar el texto actual
      if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
        el.value = val;
      } else {
        el.textContent = val;
      }
    });
    // data-i18n-title
    root.querySelectorAll("[data-i18n-title]").forEach((el) => {
      const key = el.getAttribute("data-i18n-title");
      if (!key) return;
      el.setAttribute("title", t(key));
    });
    // data-i18n-aria-label
    root.querySelectorAll("[data-i18n-aria-label]").forEach((el) => {
      const key = el.getAttribute("data-i18n-aria-label");
      if (!key) return;
      el.setAttribute("aria-label", t(key));
    });
  }

  async function bootOnce() {
    // idioma preferido (fallback 'es')
    const lang = (localStorage.getItem(LANG_KEY) || "es").toLowerCase();
    try {
      await loadLang(lang);
      applyTranslations();
    } catch (e) {
      console.error("i18n load failed:", e);
    }
  }

  // Reaplicar ante cambios desde prefs.js
  document.addEventListener("app:lang-changed", async (ev) => {
    const lang = (ev.detail && ev.detail.lang) ? String(ev.detail.lang).toLowerCase() : "es";
    if (lang === currentLang) return;
    try {
      await loadLang(lang);
      applyTranslations();
    } catch (e) {
      console.error("i18n reload failed:", e);
    }
  });

  // Exponer API mínima para otros scripts
  window.I18N = { t, apply: applyTranslations, get lang() { return currentLang; } };

  document.addEventListener("DOMContentLoaded", bootOnce);
})();
