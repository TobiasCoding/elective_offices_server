/* main.js
 * Frontend con i18n:
 *  - Filtros por año/fase.
 *  - Expandir/colapsar al click en .show-calc.
 *  - Fetch JSON externo (data-json-url).
 *  - Asignar bancas usando el ARREGLO DE SEATS enviado por el back (no lee JSONL).
 *  - Métodos soportados: d-hont / lista-incompleta / mayoria-simple / balotaje (top-2).
 *  - i18n: carga /static/i18n/{lang}.json (lang desde localStorage 'pref.lang', por defecto 'es').
 *
 * Requisitos HTML:
 *  - <tr class="data-row" data-year data-phase data-category data-office data-method data-json-url>
 *  - Luego: <tr class="calc-row" data-open="0"> con .calc-panel -> (.calc-loading, .calc-error, .calc-content)
 *  - Chips: #filter-year y #filter-phase
 *  - Back inyecta: <script id="seats-ctx" type="application/json">{ "...": N, ... }</script>
 */

/* ============================= i18n ============================= */

const LANG_KEY = "pref.lang";
let I18N = null;
let CURRENT_LANG = null;

// ---- i18n bridge: usa window.I18N.t si existe; si no, resuelve desde I18N local
function t(path) {
  // 1) Preferir la API global expuesta por static/i18n.js
  if (window.I18N && typeof window.I18N.t === "function") {
    return window.I18N.t(path);
  }
  // 2) Fallback: resolver desde el diccionario local cargado por main.js
  if (I18N && typeof path === "string") {
    if (Object.prototype.hasOwnProperty.call(I18N, path)) {
      const val = I18N[path];
      return (val === undefined || val === null) ? path : val;
    }
    // soporte a claves anidadas con puntos (p.ej. "btn.cancel")
    const parts = path.split(".");
    let node = I18N;
    for (const p of parts) {
      if (node && Object.prototype.hasOwnProperty.call(node, p)) {
        node = node[p];
      } else {
        return path;
      }
    }
    return (node === undefined || node === null) ? path : node;
  }
  // 3) Último recurso: devolver la clave (evita crashear)
  return String(path);
}

/**
 * Carga el archivo de traducciones exacto para el idioma seleccionado.
 * No implementa fallbacks.
 * Estructura esperada del JSON (ejemplo de claves usadas):
 * {
 *   "table": { "rank":"#", "group":"Grupo", "votes":"Votos", "pct":"%", "seats":"Bancas" },
 *   "meta": {
 *     "district":"Distrito", "counted":"Escrutado", "counted_suffix":"mesas",
 *     "participation":"Participación", "source":"Fuente", "official_api":"API oficial"
 *   },
 *   "method": {
 *     "dhondt":"D'Hondt", "lista_incompleta":"Lista incompleta",
 *     "mayoria_simple":"Mayoría simple", "balotaje_top2":"Balotaje • Top 2",
 *     "unsupported":"Método no soportado"
 *   },
 *   "other": {
 *     "title":"Otros",
 *     "type":"Tipo",
 *     "votes":"Votos",
 *     "pct":"%",
 *     "positive_total":"POSITIVOS",
 *     "types": { "blank":"EN BLANCO", "null":"NULO", "contested":"IMPUGNADO", "appealed":"RECURRIDO", "command":"COMANDO" }
 *   }
 * }
 */
async function loadI18n() {
  const lang = localStorage.getItem(LANG_KEY) || "es";
  const res = await fetch(`/elective_office/static/i18n/${lang}.json`, { credentials: "same-origin" });
  if (!res.ok) throw new Error(`i18n load error ${res.status} for lang=${lang}`);
  I18N = await res.json();
  CURRENT_LANG = lang;
}

/* =========================== Utilities =========================== */

async function fetchJSON(url) {
  const res = await fetch(url, { credentials: "same-origin" });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.json();
}

function normPct(x) {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function fmtInt(n) {
  return new Intl.NumberFormat("es-AR", { maximumFractionDigits: 0 }).format(n);
}
function fmtPct(x) {
  if (x === null || x === undefined || Number.isNaN(+x)) return "-";
  return new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(+x);
}

function setActiveChip(groupEl, value) {
  groupEl.querySelectorAll(".chip").forEach((ch) => {
    const isAll = ch.classList.contains("chip-all");
    ch.classList.toggle("active", isAll ? value === "" : ch.dataset.value === value);
  });
}

function getCalcPanelForRow(tr) {
  const detail = tr.nextElementSibling;
  if (!detail || !detail.classList.contains("calc-row")) {
    throw new Error("calc-row not found after data-row");
  }
  const panel = detail.querySelector(".calc-panel");
  const loading = panel.querySelector(".calc-loading");
  const error = panel.querySelector(".calc-error");
  const content = panel.querySelector(".calc-content");
  if (!panel || !loading || !error || !content) {
    throw new Error("calc panel sub-elements missing");
  }
  return { detail, panel, loading, error, content };
}

function applyFilters() {
  const y = document.querySelector("#filter-year .chip.active")?.dataset.value || "";
  const p = document.querySelector("#filter-phase .chip.active")?.dataset.value || "";
  document.querySelectorAll("#results-table tbody tr.data-row").forEach((tr) => {
    const okY = !y || tr.dataset.year === y;
    const okP = !p || tr.dataset.phase === p;
    const show = okY && okP;
    tr.style.display = show ? "" : "none";

    const detail = tr.nextElementSibling;
    if (detail && detail.classList.contains("calc-row")) {
      const isOpen = detail.dataset.open === "1";
      detail.style.display = show && isOpen ? "" : "none";
    }
  });
}

/* -------------------------- Seats (inyectado) -------------------------- */

function getSeatsCtx() {
  const el = document.getElementById("seats-ctx");
  if (!el) throw new Error("seats-ctx no inyectado");
  return JSON.parse(el.textContent || "{}");
}
const SEATS_CTX = getSeatsCtx();

/** Devuelve el número de bancas resuelto por el back para (year, category, office). */
function getSeatsFor(year, category, office) {
  const key = `${year}::${category}::${office}`;
  const v = SEATS_CTX[key];
  if (typeof v !== "number") return 0; // sin compatibilidad legacy
  return v;
}

/* --------------------------- Allocation methods --------------------------- */

function allocateDHondt(groups, seats) {
  if (!(Number.isInteger(seats) && seats > 0)) return new Map();
  const table = [];
  for (const g of groups) for (let d = 1; d <= seats; d++) table.push({ id: g.id, score: g.votes / d });
  table.sort((a, b) => b.score - a.score);
  const top = table.slice(0, seats);
  const seatMap = new Map();
  for (const row of top) seatMap.set(row.id, (seatMap.get(row.id) || 0) + 1);
  return seatMap;
}

function allocateListaIncompleta(groups, seats) {
  const seatMap = new Map();
  if (!(Number.isInteger(seats) && seats > 0)) return seatMap;
  const sorted = [...groups].sort((a, b) => b.votes - a.votes);
  if (!sorted.length) return seatMap;
  if (seats >= 3) {
    const maj = seats - 1;
    seatMap.set(sorted[0].id, maj);
    if (sorted[1]) seatMap.set(sorted[1].id, 1);
  } else {
    seatMap.set(sorted[0].id, seats);
  }
  return seatMap;
}

function allocateMayoriaSimple(groups, seats) {
  const seatMap = new Map();
  if (!(Number.isInteger(seats) && seats > 0)) return seatMap;
  const sorted = [...groups].sort((a, b) => b.votes - a.votes);
  if (sorted[0]) seatMap.set(sorted[0].id, seats);
  return seatMap;
}

function computeBalotajeTop2(groups) {
  const sorted = [...groups].sort((a, b) => b.votes - a.votes);
  return sorted.slice(0, 2);
}

/* ------------------------------ Rendering ------------------------------ */

function renderResultsTable(groups, seatMap, showSeats) {
  const head = `
    <thead>
      <tr>
        <th>${t("table.rank")}</th>
        <th>${t("table.group")}</th>
        <th>${t("table.votes")}</th>
        <th>${t("table.pct")}</th>
        ${showSeats ? `<th>${t("table.seats")}</th>` : ""}
      </tr>
    </thead>`;
  const bodyRows = groups.map((g, i) => `
      <tr>
        <td>${i + 1}</td>
        <td>${g.name}</td>
        <td>${fmtInt(g.votes)}</td>
        <td>${fmtPct(g.pct)}</td>
        ${showSeats ? `<td>${seatMap.get(g.id) || 0}</td>` : ""}
      </tr>`).join("");
  return `<table class="mini-table">${head}<tbody>${bodyRows}</tbody></table>`;
}

// Muestra POSITIVOS y "otros" con % sobre el total de votos
function renderOthersTable(api) {
  const totalPos = Number(api.positivos ?? 0);

  const pairs = [
    [t("other.types.blank"), api["EN BLANCO"]],
    [t("other.types.null"), api["NULO"]],
    [t("other.types.contested"), api["IMPUGNADO"]],
    [t("other.types.appealed"), api["RECURRIDO"]],
    [t("other.types.command"), api["COMANDO"]],
  ].filter(([, v]) => v !== undefined && v !== null);

  const totalOtros = pairs.reduce((acc, [, v]) => acc + (Number(v) || 0), 0);
  const totalVotos = totalPos + totalOtros;

  const pct = (n) => (totalVotos > 0 ? (n * 100) / totalVotos : 0);

  const totalRow = `<tr class="total">
    <td><strong>${t("other.positive_total")}</strong></td>
    <td style="text-align:right"><strong>${fmtInt(totalPos)}</strong></td>
    <td style="text-align:right"><strong>${fmtPct(pct(totalPos))}</strong></td>
  </tr>`;

  const rows = pairs.map(([k, v]) => {
    const n = Number(v) || 0;
    return `<tr>
      <td>${k}</td>
      <td style="text-align:right">${fmtInt(n)}</td>
      <td style="text-align:right">${fmtPct(pct(n))}</td>
    </tr>`;
  }).join("");

  return `<table class="mini-table">
    <thead><tr><th>${t("other.type")}</th><th>${t("other.votes")}</th><th>${t("other.pct")}</th></tr></thead>
    <tbody>${totalRow}${rows}</tbody>
  </table>`;
}

/* ------------------------------ Controller ------------------------------ */

async function handleComputeForRow(tr) {
  const { detail, loading, error, content } = getCalcPanelForRow(tr);

  const year = tr.dataset.year;
  const category = tr.dataset.category;
  const phase = tr.dataset.phase;
  const office = tr.dataset.office;
  const method = tr.dataset.method;
  const jsonUrl = tr.dataset.jsonUrl;

  if (!year || !category || !phase || !office || !method || !jsonUrl) {
    throw new Error("falta algún data-* requerido en la fila");
  }

  loading.hidden = false;
  error.hidden = true;
  content.hidden = true;
  content.innerHTML = "";

  // API oficial
  const api = await fetchJSON(jsonUrl);

  // Grupos (solo POSITIVOS)
  const totalPos = Number(api.positivos ?? 0);
  const groups = (api.agrupaciones || []).map((a) => {
    const votes = Number(a.votos);
    const pct = a.porcentaje !== undefined ? normPct(a.porcentaje) : (totalPos ? (votes * 100) / totalPos : null);
    return { id: String(a.idAgrupacion ?? a.id ?? a.nombre), name: String(a.nombre), votes, pct: pct ?? 0 };
  });

  // Seats ya resueltos por el back
  const seats = getSeatsFor(year, category, office);

  // Asignación según método
  let seatMap = new Map();
  let showSeats = false;
  let headerNote = "";

  switch (method) {
    case "d-hont":
      seatMap = allocateDHondt(groups, seats);
      showSeats = seats > 0;
      headerNote = `${t("method.dhondt")} • ${seats} ${t("table.seats")}`;
      break;
    case "lista-incompleta":
      seatMap = allocateListaIncompleta(groups, seats);
      showSeats = seats > 0;
      headerNote = `${t("method.lista_incompleta")} • ${seats} ${t("table.seats")}`;
      break;
    case "mayoria-simple":
      seatMap = allocateMayoriaSimple(groups, seats);
      showSeats = seats > 0;
      headerNote = `${t("method.mayoria_simple")} • ${seats} ${t("table.seats")}`;
      break;
    case "balotaje": {
      // Aplicar lógica de balotaje SOLO si la fase actual es BALOTAJE
      if ((phase || "").toUpperCase() === "BALOTAJE") {
        const top2 = computeBalotajeTop2(groups);
        headerNote = t("method.balotaje_top2");
        const balotajeTable = renderResultsTable(top2, new Map(), false);
        const meta = `
          <div class="meta">
            <div class="pill">${t("meta.district")}: <strong>${api.Distrito || "-"}</strong></div>
            <div class="pill">${t("meta.counted")}: <strong>${fmtInt(api.MesasEscrutadas || 0)} ${t("meta.counted_suffix")}</strong></div>
            <div class="pill">${t("meta.participation")}: <strong>${fmtPct(api.ParticipacionSobreEscrutado || 0)}%</strong></div>
            <div class="pill">${t("meta.source")}: <a href="${jsonUrl}" target="_blank" rel="noopener">${t("meta.official_api")}</a></div>
          </div>`;
        const others = renderOthersTable(api);
        content.innerHTML = `
          <div class="calc-header">
            <div><strong>${year} • ${category} • ${phase}</strong></div>
            <div>${office} — ${headerNote}</div>
          </div>
          ${meta}
          ${balotajeTable}
          <h4>${t("other.title")}</h4>
          ${others}
        `;
        loading.hidden = true;
        content.hidden = false;
        detail.dataset.loaded = "1";
        return;
      }
      // En PASO o GENERAL no corresponde la lógica de balotaje: usar render común
      headerNote = `${phase}`;
      showSeats = false;
      seatMap = new Map();
      break;
    }
    default:
      headerNote = `${t("method.unsupported")}: ${method}`;
  }

  // Render común
  const sorted = [...groups].sort((a, b) => b.votes - a.votes);
  const tableHTML = renderResultsTable(sorted, seatMap, showSeats);
  const meta = `
    <div class="meta">
      <div class="pill">${t("meta.district")}: <strong>${api.Distrito || "-"}</strong></div>
      <div class="pill">${t("meta.counted")}: <strong>${fmtInt(api.MesasEscrutadas || 0)} ${t("meta.counted_suffix")}</strong></div>
      <div class="pill">${t("meta.participation")}: <strong>${fmtPct(api.ParticipacionSobreEscrutado || 0)}%</strong></div>
      <div class="pill">${t("meta.source")}: <a href="${jsonUrl}" target="_blank" rel="noopener">${t("meta.official_api")}</a></div>
    </div>`;
  const others = renderOthersTable(api);

  content.innerHTML = `
    <div class="calc-header">
      <div><strong>${year} • ${category} • ${phase}</strong></div>
      <div>${office} — ${headerNote}</div>
    </div>
    ${meta}
    ${tableHTML}
    <h4>${t("other.title")}</h4>
    ${others}
  `;

  loading.hidden = true;
  content.hidden = false;
  detail.dataset.loaded = "1";
}

async function toggleDetailForRow(tr) {
  const { detail, loading, error, content } = getCalcPanelForRow(tr);
  const isOpen = detail.dataset.open === "1";

  if (isOpen) {
    detail.dataset.open = "0";
    detail.style.display = "none";
    applyFilters();
    return;
  }

  detail.dataset.open = "1";
  detail.style.display = "";

  // Si nunca se cargó, computa y renderiza
  if (detail.dataset.loaded !== "1") {
    loading.hidden = false;
    error.hidden = true;
    content.hidden = true;

    try {
      await handleComputeForRow(tr);
      error.hidden = true;
      content.hidden = false;
      detail.dataset.loaded = "1";
    } catch (err) {
      console.error(err);
      error.textContent = String(err.message || err);
      error.hidden = false;
      content.hidden = true;
    } finally {
      loading.hidden = true;
    }
  }

  applyFilters();
}


/* ------------------------------- Boot ------------------------------- */

async function retranslateOpenCalcs() {
  // Re-renderiza las filas ya cargadas y abiertas para reflejar el nuevo idioma
  const rows = document.querySelectorAll("#results-table tbody tr.data-row");
  for (const tr of rows) {
    const detail = tr.nextElementSibling;
    if (!detail || !detail.classList.contains("calc-row")) continue;
    const isOpen = detail.dataset.open === "1";
    const isLoaded = detail.dataset.loaded === "1";
    if (isLoaded && isOpen) {
      // vuelve a calcular/armar la UI con las cadenas del nuevo idioma
      await handleComputeForRow(tr);
      detail.dataset.open = "1"; // asegurar abierto
      applyFilters();
    }
  }
}

document.addEventListener("app:lang-changed", async (ev) => {
  const lang = ev?.detail?.lang;
  if (!lang || lang === CURRENT_LANG) return;
  await loadI18n();
  await retranslateOpenCalcs();
});

document.addEventListener("DOMContentLoaded", async () => {
  // Cargar i18n antes de enganchar eventos que construyen HTML con texto
  await loadI18n();

  setActiveChip(document.getElementById("filter-year"), "");
  setActiveChip(document.getElementById("filter-phase"), "");
  applyFilters();

  document.getElementById("filter-year")?.addEventListener("click", (ev) => {
    const btn = ev.target.closest(".chip"); if (!btn) return;
    setActiveChip(ev.currentTarget, btn.dataset.value || ""); applyFilters();
  });
  document.getElementById("filter-phase")?.addEventListener("click", (ev) => {
    const btn = ev.target.closest(".chip"); if (!btn) return;
    setActiveChip(ev.currentTarget, btn.dataset.value || ""); applyFilters();
  });

  document.querySelector("#results-table tbody")?.addEventListener("click", async (ev) => {
    const btn = ev.target.closest(".sync-btn");
    if (!btn) return;
    const tr = btn.closest("tr.data-row");
    if (!tr) return;

    const { loading, error, content } = getCalcPanelForRow(tr);
    btn.disabled = true; btn.classList.add("is-loading");
    loading.hidden = false; error.hidden = true; content.hidden = true;

    try {
      await handleComputeForRow(tr);
      error.hidden = true; content.hidden = false;
      const { detail } = getCalcPanelForRow(tr);
      detail.dataset.open = "1"; applyFilters();
    } catch (err) {
      console.error(err);
      error.textContent = String(err.message || err);
      error.hidden = false; content.hidden = true;
    } finally {
      loading.hidden = true; btn.classList.remove("is-loading"); btn.disabled = false;
    }
  });

  document.querySelector("#results-table tbody")?.addEventListener("click", async (ev) => {
    const link = ev.target.closest(".show-calc");
    if (!link) return;
    const tr = link.closest("tr.data-row");
    if (!tr) return;
    await toggleDetailForRow(tr);
  });
});
