/* static/prefs.js
 * Preferencias de UI persistentes en localStorage:
 *  - Tema: 'light' | 'dark'  (clave: 'pref.theme')
 *  - Idioma: 'es' | 'en' ... (clave: 'pref.lang')
 * Aplica clase 'theme-dark' en <html> para modo oscuro.
 */

(function () {
  "use strict";

  const THEME_KEY = "pref.theme";
  const LANG_KEY  = "pref.lang";

  // ---------- Helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);

  function getPref(key, fallback) {
    try {
      const val = localStorage.getItem(key);
      return val ?? fallback;
    } catch {
      return fallback;
    }
  }

  function setPref(key, val) {
    localStorage.setItem(key, val);
  }

  function applyTheme(theme) {
    const html = document.documentElement;
    const isDark = theme === "dark";
    html.classList.toggle("theme-dark", isDark);

    // Accesibilidad: aria-pressed en toggle
    const btn = $("#theme-toggle");
    if (btn) {
      btn.setAttribute("aria-pressed", String(isDark));
      const icon = btn.querySelector(".theme-icon");
      const label = btn.querySelector(".theme-label");
      if (icon) icon.textContent = isDark ? (icon.dataset.dark || "🌙") : (icon.dataset.light || "☀️");
      if (label) label.textContent = isDark ? "Dark" : "Light";
    }

    // Notificar a la app si algo desea reaccionar
    document.dispatchEvent(new CustomEvent("app:theme-changed", { detail: { theme } }));
  }

  function applyLang(lang) {
    const sel = $("#lang-select");
    if (sel && sel.value !== lang) sel.value = lang;

    // Notificar a la app: aún NO traducimos textos; solo persistimos y avisamos
    document.dispatchEvent(new CustomEvent("app:lang-changed", { detail: { lang } }));
  }

  // ---------- Boot ----------
  document.addEventListener("DOMContentLoaded", () => {
    // Defaults: light / es
    const theme = getPref(THEME_KEY, "light");
    const lang  = getPref(LANG_KEY, "es");

    applyTheme(theme);
    applyLang(lang);

    // Toggle de tema
    const btn = $("#theme-toggle");
    if (btn) {
      btn.addEventListener("click", () => {
        const next = document.documentElement.classList.contains("theme-dark") ? "light" : "dark";
        setPref(THEME_KEY, next);
        applyTheme(next);
      });
    }

    // Selector de idioma
    const sel = $("#lang-select");
    if (sel) {
      sel.addEventListener("change", () => {
        const val = sel.value || "es";
        setPref(LANG_KEY, val);
        applyLang(val);
      });
    }
  });
})();
