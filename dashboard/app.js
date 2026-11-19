/* Dukaan Retail sales dashboard.
 *
 * Static, no build step. Fetches the JSON written by export_data.py from ./data/
 * and renders KPI cards, a monthly Chart.js line chart, a sortable store league
 * table with SVG sparklines, category bars, a channel x payment mix and the
 * data-quality panel. Every value read from the data is inserted with
 * textContent, never innerHTML.
 */
(function () {
  "use strict";

  const DATA_FILES = ["meta", "summary", "monthly", "stores", "categories", "channels", "quality"];
  const ALL = "all";
  const PAYMENT_ORDER = ["cash", "zaad", "edahab", "card"]; // fixed slot order: colour follows the entity
  const PAYMENT_NAMES = { cash: "Cash", zaad: "Zaad", edahab: "eDahab", card: "Card" };
  const CHANNEL_NAMES = { in_store: "In store", mobile_app: "Mobile app", phone: "Phone" };
  const REPO_URL = "https://github.com/mahamed7/retail-data-warehouse";
  const THEME_KEY = "dukaan-theme";
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const darkQuery = window.matchMedia("(prefers-color-scheme: dark)");

  const state = {
    data: null,
    year: ALL,
    metric: "revenue",
    sort: { key: "revenue", dir: "desc" },
    chart: null,
  };

  // ------------------------------------------------------------------ formatting
  const nf = (opts) => new Intl.NumberFormat("en-US", opts);
  const F = {
    usd: nf({ style: "currency", currency: "USD", maximumFractionDigits: 0 }),
    usd2: nf({ style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    usdCompact: nf({ style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }),
    int: nf({ maximumFractionDigits: 0 }),
    pct: nf({ style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 }),
    pct0: nf({ style: "percent", maximumFractionDigits: 0 }),
    signedPct: nf({ style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1, signDisplay: "exceptZero" }),
    signed1: nf({ minimumFractionDigits: 1, maximumFractionDigits: 1, signDisplay: "exceptZero" }),
  };
  const dash = "–";
  const fmt = (formatter, value) => (value === null || value === undefined || Number.isNaN(value) ? dash : formatter.format(value));
  const usd = (v) => fmt(F.usd, v);
  const usd2 = (v) => fmt(F.usd2, v);
  const usdCompact = (v) => (v !== null && v !== undefined && Math.abs(v) < 1000 ? usd(v) : fmt(F.usdCompact, v));
  const int = (v) => fmt(F.int, v);
  const pct = (v) => fmt(F.pct, v);
  const signedPct = (v) => fmt(F.signedPct, v);
  const pts = (v) => (v === null || v === undefined ? dash : `${F.signed1.format(v * 100)} pts`);
  const channelName = (c) => CHANNEL_NAMES[c] || c;
  const paymentName = (p) => PAYMENT_NAMES[p] || p;

  const monthFmt = new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
  const monthLabel = (ym) => monthFmt.format(new Date(`${ym}-01T00:00:00Z`));

  const METRICS = {
    revenue: { name: "revenue", axis: usdCompact, value: usd, sub: "Net revenue from completed orders, by calendar month.", zero: true },
    margin_pct: { name: "gross margin %", axis: (v) => fmt(F.pct0, v), value: pct, sub: "Gross margin as a share of revenue with a known product cost.", zero: false },
    completed_orders: { name: "orders", axis: int, value: int, sub: "Completed orders, by calendar month.", zero: true },
  };

  // ------------------------------------------------------------------ DOM helpers
  function h(tag, attrs, ...children) {
    const el = document.createElement(tag);
    applyAttrs(el, attrs);
    for (const child of children.flat()) {
      if (child === null || child === undefined || child === false) continue;
      el.append(child instanceof Node ? child : String(child));
    }
    return el;
  }

  function s(tag, attrs, ...children) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    applyAttrs(el, attrs);
    for (const child of children.flat()) if (child) el.append(child);
    return el;
  }

  function applyAttrs(el, attrs) {
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value === null || value === undefined || value === false) continue;
      if (key === "class") el.setAttribute("class", value);
      else if (key === "text") el.textContent = value;
      else if (key === "style") el.style.cssText = value;
      else if (key.startsWith("on") && typeof value === "function") el.addEventListener(key.slice(2), value);
      else el.setAttribute(key, value === true ? "" : value);
    }
  }

  const $ = (id) => document.getElementById(id);
  const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  function arrowIcon(direction) {
    const d = direction === "up" ? "M6 2.5 10 8H2z" : direction === "down" ? "M6 9.5 2 4h8z" : "M2 6h8";
    return s("svg", { viewBox: "0 0 12 12", "aria-hidden": "true" },
      s("path", { d, fill: direction === "flat" ? "none" : "currentColor", stroke: "currentColor", "stroke-width": direction === "flat" ? "2" : "0" }));
  }

  // ------------------------------------------------------------------ tooltip
  const tip = $("tip");

  function showTip(lines, x, y) {
    tip.replaceChildren(...lines.map((line, i) => h("div", { class: i === 0 ? null : "tip-muted" }, i === 0 ? h("strong", { text: line }) : line)));
    tip.classList.add("show");
    const pad = 12;
    const rect = tip.getBoundingClientRect();
    let left = x + pad;
    let top = y + pad;
    if (left + rect.width > window.innerWidth - 8) left = Math.max(8, x - rect.width - pad);
    if (top + rect.height > window.innerHeight - 8) top = Math.max(8, y - rect.height - pad);
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
  }

  function hideTip() {
    tip.classList.remove("show");
  }

  /** Hover and keyboard-focus tooltip for a mark; the same text is its accessible name. */
  function attachTip(el, lines) {
    el.setAttribute("tabindex", "0");
    el.setAttribute("aria-label", lines.join(", "));
    el.addEventListener("pointermove", (e) => showTip(lines, e.clientX, e.clientY));
    el.addEventListener("pointerleave", hideTip);
    el.addEventListener("focus", () => {
      const r = el.getBoundingClientRect();
      showTip(lines, r.left + r.width / 2, r.bottom);
    });
    el.addEventListener("blur", hideTip);
  }

  // ------------------------------------------------------------------ loading
  async function loadJSON(name) {
    const response = await fetch(`data/${name}.json`, { cache: "no-cache" });
    if (!response.ok) throw new Error(`data/${name}.json returned HTTP ${response.status}`);
    return response.json();
  }

  function showError(error) {
    const main = $("main");
    main.classList.remove("is-loading");
    main.classList.add("has-error");
    document.querySelector(".filter-bar").hidden = true;
    main.setAttribute("aria-busy", "false");
    const fromDisk = window.location.protocol === "file:";
    $("error-slot").replaceChildren(
      h("div", { class: "notice", role: "alert" },
        h("h2", { text: "The dashboard data couldn't be loaded" }),
        h("p", {
          text: fromDisk
            ? "Browsers block data requests from pages opened straight from disk. Serve the folder instead: run python -m http.server in dashboard/ and open http://localhost:8000."
            : "The JSON files the warehouse exports are missing or unreadable. Try reloading in a minute; if it persists, the latest CI run may have failed.",
        }),
        h("p", {}, h("small", { text: `Details: ${error && error.message ? error.message : error}` }))));
    $("build-line").replaceChildren(h("span", { text: "Build info unavailable." }));
  }

  // ------------------------------------------------------------------ theme
  function effectiveTheme() {
    const explicit = document.documentElement.getAttribute("data-theme");
    if (explicit === "light" || explicit === "dark") return explicit;
    return darkQuery.matches ? "dark" : "light";
  }

  function syncThemeButton() {
    const next = effectiveTheme() === "dark" ? "light" : "dark";
    $("theme-toggle").setAttribute("aria-label", `Switch to ${next} theme`);
    $("theme-toggle").setAttribute("title", `Switch to ${next} theme`);
  }

  function setupTheme() {
    syncThemeButton();
    $("theme-toggle").addEventListener("click", () => {
      const next = effectiveTheme() === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      try { localStorage.setItem(THEME_KEY, next); } catch (e) { /* not persisted; fine */ }
      syncThemeButton();
      if (state.data) renderMonthly();
    });
    darkQuery.addEventListener("change", () => {
      syncThemeButton();
      if (state.data && !document.documentElement.hasAttribute("data-theme")) renderMonthly();
    });
  }

  // ------------------------------------------------------------------ header
  function renderBuildLine() {
    const meta = state.data.meta || {};
    const line = $("build-line");
    const parts = [];
    if (meta.source === "sample") {
      parts.push(h("span", { class: "badge", text: "Sample data" }));
      parts.push(h("span", { text: "Computed locally from the synthetic CSVs; CI replaces it on the next push to main." }));
    } else {
      parts.push(h("span", { class: "badge badge-live", text: "Built by CI" }));
    }
    if (meta.generated_at) {
      const when = new Date(meta.generated_at);
      const text = Number.isNaN(when.getTime())
        ? meta.generated_at
        : `${new Intl.DateTimeFormat("en-GB", { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" }).format(when)} UTC`;
      parts.push(h("span", {}, "Last built ", h("time", { datetime: meta.generated_at, text })));
    }
    const repoUrl = meta.repository ? `https://github.com/${meta.repository}` : REPO_URL;
    if (meta.git_sha) {
      parts.push(h("span", {}, "Commit ",
        h("a", { href: `${repoUrl}/commit/${encodeURIComponent(meta.git_sha)}` }, h("code", { text: String(meta.git_sha).slice(0, 7) }))));
    }
    if (meta.run_url && /^https:\/\/github\.com\//.test(meta.run_url)) {
      parts.push(h("a", { href: meta.run_url, text: "CI run" }));
    }
    line.replaceChildren(...parts);
  }

  // ------------------------------------------------------------------ filters
  function setupFilters() {
    const years = (state.data.summary.years || []).map(String);
    const group = $("year-filter");
    const options = [ALL, ...years];
    const fromHash = new URLSearchParams(window.location.hash.slice(1)).get("year");
    if (fromHash && options.includes(fromHash)) state.year = fromHash;

    group.replaceChildren(...options.map((value) =>
      h("button", { type: "button", role: "radio", "data-year": value, text: value === ALL ? "All" : value })));
    const buttons = Array.from(group.querySelectorAll("button"));
    const select = (value, focus) => {
      state.year = value;
      buttons.forEach((b) => {
        const on = b.dataset.year === value;
        b.setAttribute("aria-checked", String(on));
        b.tabIndex = on ? 0 : -1;
        if (on && focus) b.focus();
      });
      try { history.replaceState(null, "", value === ALL ? window.location.pathname + window.location.search : `#year=${value}`); } catch (e) { /* ignore */ }
      renderFiltered();
    };
    buttons.forEach((b, i) => {
      b.addEventListener("click", () => select(b.dataset.year, false));
      b.addEventListener("keydown", (e) => {
        const step = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : e.key === "ArrowLeft" || e.key === "ArrowUp" ? -1 : 0;
        if (!step) return;
        e.preventDefault();
        select(buttons[(i + step + buttons.length) % buttons.length].dataset.year, true);
      });
    });
    buttons.forEach((b) => {
      const on = b.dataset.year === state.year;
      b.setAttribute("aria-checked", String(on));
      b.tabIndex = on ? 0 : -1;
    });

    const metricButtons = Array.from($("metric-toggle").querySelectorAll("button"));
    metricButtons.forEach((b) => b.addEventListener("click", () => {
      state.metric = b.dataset.metric;
      metricButtons.forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      renderMonthly();
    }));
  }

  function periodLabel() {
    return state.year === ALL ? "2024–2025" : state.year;
  }

  // ------------------------------------------------------------------ KPIs
  function delta(current, previous, { kind = "relative", upIsGood = true, neutral = false } = {}) {
    if (current === null || current === undefined || previous === null || previous === undefined) return null;
    const change = kind === "relative" ? (previous === 0 ? null : (current - previous) / previous) : current - previous;
    if (change === null) return null;
    const direction = Math.abs(change) < 0.0005 ? "flat" : change > 0 ? "up" : "down";
    const tone = neutral || direction === "flat" ? "flat" : (direction === "up") === upIsGood ? "good" : "bad";
    const text = kind === "relative" ? signedPct(change) : pts(change);
    const words = direction === "flat" ? "unchanged" : `${direction} ${text.replace(/^[+-]/, "")}`;
    return { direction, tone, text, words };
  }

  function kpiCard({ label, value, title, foot, change, compareYear, hero }) {
    const footer = h("div", { class: "kpi-foot" });
    if (change) {
      footer.append(
        h("span", { class: `delta ${change.tone}`, "aria-hidden": "true" }, arrowIcon(change.direction), change.text),
        h("span", { "aria-hidden": "true", text: `vs ${compareYear}` }),
        h("span", { class: "sr-only", text: `${change.words} versus ${compareYear}.` }));
    }
    if (foot) footer.append(h("span", { text: foot }));
    return h("article", { class: `kpi${hero ? " kpi-hero" : ""}` },
      h("h3", { class: "kpi-label", style: "margin:0", text: label }),
      h("p", { class: "kpi-value", style: "margin:0", title: title || null, text: value }),
      footer);
  }

  function renderKpis() {
    const summary = state.data.summary;
    const k = summary.periods[state.year];
    if (!k) return;
    const prevYear = state.year === ALL ? null : String(Number(state.year) - 1);
    const p = prevYear ? summary.periods[prevYear] : null;
    const d = (key, opts) => (p ? delta(k[key], p[key], opts) : null);
    const cards = [
      { label: `Revenue, ${periodLabel()}`, value: usdCompact(k.revenue), title: usd2(k.revenue), hero: true,
        change: d("revenue"), foot: `${int(k.units_sold)} units sold` },
      { label: "Gross margin", value: pct(k.margin_pct), change: d("margin_pct", { kind: "points" }),
        foot: `${usdCompact(k.gross_margin)} gross profit` },
      { label: "Completed orders", value: int(k.completed_orders), change: d("completed_orders"),
        foot: `of ${int(k.total_orders)} placed` },
      { label: "Average order value", value: usd2(k.avg_order_value), change: d("avg_order_value") },
      { label: "Return rate", value: pct(k.returned_rate), change: d("returned_rate", { kind: "points", upIsGood: false }),
        foot: `${int(k.returned_orders)} orders returned` },
      { label: "Walk-in share", value: pct(k.walk_in_share), change: d("walk_in_share", { kind: "points", neutral: true }),
        foot: "orders without a customer account" },
    ];
    const yoy = summary.yoy;
    if (yoy) {
      cards.push({
        label: `Revenue growth, ${yoy.current_year} vs ${yoy.previous_year}`,
        value: signedPct(yoy.revenue_growth),
        foot: `Orders ${signedPct(yoy.orders_growth)} · AOV ${signedPct(yoy.aov_growth)} · margin ${pts(yoy.margin_pct_change)}`,
      });
    }
    $("kpis").replaceChildren(...cards.map((c) => kpiCard({ ...c, compareYear: prevYear })));
  }

  // ------------------------------------------------------------------ monthly chart
  const endLabels = {
    id: "endLabels",
    afterDatasetsDraw(chart) {
      const points = chart.data.datasets.map((ds, i) => {
        const meta = chart.getDatasetMeta(i);
        const last = meta.data[meta.data.length - 1];
        return last && !meta.hidden ? { label: ds.label, x: last.x, y: last.y, alpha: ds.labelAlpha } : null;
      }).filter(Boolean);
      // Converging lines: skip the end labels rather than nudging them off their lines.
      for (let i = 0; i < points.length; i++) {
        for (let j = i + 1; j < points.length; j++) if (Math.abs(points[i].y - points[j].y) < 14) return;
      }
      const { ctx } = chart;
      ctx.save();
      ctx.font = `600 12px ${cssVar("--font") || "system-ui"}`;
      ctx.fillStyle = cssVar("--ink-2");
      ctx.textBaseline = "middle";
      for (const p of points) {
        ctx.globalAlpha = p.alpha;
        ctx.fillText(p.label, p.x + 9, p.y);
      }
      ctx.restore();
    },
  };

  function monthlySeries() {
    const months = state.data.monthly.months || [];
    const years = Array.from(new Set(months.map((m) => m.year))).sort();
    const labels = [];
    months.forEach((m) => { if (!labels[m.month_num - 1]) labels[m.month_num - 1] = m.label; });
    const series = years.map((year) => {
      const values = new Array(12).fill(null);
      months.filter((m) => m.year === year).forEach((m) => { values[m.month_num - 1] = m[state.metric]; });
      return { year, values };
    });
    return { labels, series, years };
  }

  function renderMonthly() {
    const metric = METRICS[state.metric];
    $("monthly-metric-name").textContent = metric.name;
    $("monthly-sub").textContent = metric.sub;
    const { labels, series, years } = monthlySeries();
    const latest = years[years.length - 1];
    const accent = cssVar("--accent");
    const prior = cssVar("--prior");
    const surface = cssVar("--surface");

    $("monthly-legend").replaceChildren(...series.slice().reverse().map(({ year }) =>
      h("li", {}, h("span", { class: "key-line", style: `color:${year === latest ? accent : prior}` }), String(year))));

    const totals = series.map(({ year, values }) => `${year}: ${values.filter((v) => v !== null).length} months`).join(", ");
    $("monthly-chart").setAttribute("aria-label",
      `Line chart of monthly ${metric.name} for ${years.join(" and ")} (${totals}). The table below lists every value.`);

    if (typeof window.Chart === "undefined") {
      const box = $("monthly-chart").parentElement;
      box.style.height = "auto";
      box.replaceChildren(h("p", { class: "chart-fallback", text: "The charting library couldn't be loaded, so the monthly figures are shown as a table instead." }));
      document.querySelector("details.table-view").open = true;
      return;
    }

    const datasets = series.map(({ year, values }) => {
      const isLatest = year === latest;
      const dim = state.year !== ALL && String(year) !== state.year;
      const colour = isLatest ? accent : prior;
      const alpha = dim ? 0.3 : 1;
      const lastIndex = values.length - 1;
      return {
        label: String(year),
        data: values,
        borderColor: withAlpha(colour, alpha),
        backgroundColor: withAlpha(colour, alpha),
        borderWidth: 2,
        borderCapStyle: "round",
        borderJoinStyle: "round",
        tension: 0,
        pointRadius: values.map((_, i) => (i === lastIndex ? 4 : 0)),
        pointHoverRadius: 5,
        pointBorderColor: surface,
        pointBorderWidth: 2,
        pointHoverBorderWidth: 2,
        pointHitRadius: 12,
        labelAlpha: alpha,
        order: isLatest ? 0 : 1,
      };
    });

    if (state.chart) state.chart.destroy();
    window.Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
    state.chart = new window.Chart($("monthly-chart"), {
      type: "line",
      data: { labels, datasets },
      plugins: [endLabels],
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: reducedMotion ? false : { duration: 250 },
        interaction: { mode: "index", intersect: false },
        layout: { padding: { top: 8, right: 40 } },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: surface,
            titleColor: cssVar("--ink"),
            bodyColor: cssVar("--ink-2"),
            footerColor: cssVar("--muted"),
            borderColor: cssVar("--axis"),
            borderWidth: 1,
            padding: 10,
            boxWidth: 14,
            boxHeight: 2,
            boxPadding: 4,
            itemSort: (a, b) => Number(b.dataset.label) - Number(a.dataset.label),
            callbacks: {
              label: (ctx) => ` ${metric.value(ctx.parsed.y)}  ${ctx.dataset.label}`,
              labelColor: (ctx) => ({ borderColor: ctx.dataset.borderColor, backgroundColor: ctx.dataset.borderColor }),
              footer: (items) => {
                if (items.length < 2) return "";
                const byYear = Object.fromEntries(items.map((it) => [it.dataset.label, it.parsed.y]));
                const cur = byYear[String(latest)];
                const prev = byYear[String(latest - 1)];
                if (cur == null || prev == null) return "";
                return state.metric === "margin_pct" ? `Change: ${pts(cur - prev)}` : prev ? `Change: ${signedPct((cur - prev) / prev)}` : "";
              },
            },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            border: { color: cssVar("--axis") },
            ticks: { color: cssVar("--muted"), maxRotation: 0, autoSkipPadding: 8 },
          },
          y: {
            beginAtZero: metric.zero,
            grid: { color: cssVar("--grid"), drawTicks: false },
            border: { display: false },
            ticks: { color: cssVar("--muted"), padding: 8, maxTicksLimit: 5, callback: (v) => metric.axis(v) },
          },
        },
      },
    });
  }

  /** Apply an alpha to a #rrggbb colour (Chart.js needs a concrete colour, not a CSS var). */
  function withAlpha(hex, alpha) {
    if (alpha >= 1 || !/^#[0-9a-f]{6}$/i.test(hex)) return hex;
    const n = parseInt(hex.slice(1), 16);
    return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
  }

  function renderMonthlyTable() {
    const months = state.data.monthly.months || [];
    const cols = [
      ["Month", (m) => monthLabel(m.month), false],
      ["Revenue", (m) => usd2(m.revenue), true],
      ["Gross margin", (m) => usd2(m.gross_margin), true],
      ["Margin %", (m) => pct(m.margin_pct), true],
      ["Orders", (m) => int(m.completed_orders), true],
      ["AOV", (m) => usd2(m.avg_order_value), true],
    ];
    $("monthly-table").replaceChildren(h("table", { class: "compact" },
      h("caption", { class: "sr-only", text: "Monthly revenue, margin and completed orders" }),
      h("thead", {}, h("tr", {}, cols.map(([name, , num]) => h("th", { scope: "col", class: num ? "num" : null, text: name })))),
      h("tbody", {}, months.map((m) => h("tr", {}, cols.map(([, get, num], i) =>
        i === 0 ? h("th", { scope: "row", style: "font-weight:500", text: get(m) }) : h("td", { class: num ? "num" : null, text: get(m) })))))));
  }

  // ------------------------------------------------------------------ stores
  const STORE_COLUMNS = [
    { key: "store_name", label: "Store", type: "text" },
    { key: "revenue", label: "Revenue", type: "num" },
    { key: "margin_pct", label: "Margin", type: "num" },
    { key: "avg_order_value", label: "AOV", type: "num", title: "Average order value", cls: "col-hide-sm" },
    { key: "completed_orders", label: "Orders", type: "num", cls: "col-hide-sm" },
    { key: "returned_rate", label: "Returns", type: "num", title: "Return rate: returned / (completed + returned) orders", cls: "col-hide-xs" },
    { key: "trend", label: "Trend", type: "none" },
  ];

  function sparkline(values, months, year) {
    const w = 104, hgt = 30, pad = 3;
    const nums = values.map((v) => (v == null ? 0 : v));
    const min = Math.min(...nums), max = Math.max(...nums);
    const span = max - min || 1;
    const x = (i) => pad + (i * (w - 2 * pad)) / Math.max(1, nums.length - 1);
    const y = (v) => hgt - pad - ((v - min) / span) * (hgt - 2 * pad);
    const path = (idx) => idx.map((i, n) => `${n ? "L" : "M"}${x(i).toFixed(1)},${y(nums[i]).toFixed(1)}`).join("");
    const all = nums.map((_, i) => i);
    const highlighted = year === ALL ? all : all.filter((i) => (months[i] || "").startsWith(year));
    const lastIdx = highlighted[highlighted.length - 1];
    const peak = nums.indexOf(max);
    const label = `Monthly revenue ${monthLabel(months[0])} to ${monthLabel(months[months.length - 1])}: from ${usd(nums[0])} to ${usd(nums[nums.length - 1])}, peak ${usd(max)} in ${monthLabel(months[peak])}`;
    return s("svg", { class: "spark", width: w, height: hgt, viewBox: `0 0 ${w} ${hgt}`, role: "img", "aria-label": label },
      s("title", {}, document.createTextNode(label)),
      year === ALL ? null : s("path", { d: path(all), fill: "none", style: "stroke:var(--prior)", "stroke-width": "1.25", "vector-effect": "non-scaling-stroke", "stroke-linejoin": "round", "stroke-linecap": "round", opacity: "0.7" }),
      highlighted.length ? s("path", { d: path(highlighted), fill: "none", style: "stroke:var(--accent)", "stroke-width": "2", "vector-effect": "non-scaling-stroke", "stroke-linejoin": "round", "stroke-linecap": "round" }) : null,
      lastIdx !== undefined ? s("circle", { cx: x(lastIdx), cy: y(nums[lastIdx]), r: "2.75", style: "fill:var(--accent);stroke:var(--surface)", "stroke-width": "1.5" }) : null);
  }

  function renderStores() {
    const { stores = [], months = [] } = state.data.stores;
    const rows = stores
      .map((store) => ({ ...store, ...(store.periods[state.year] || {}) }))
      .filter((r) => r.revenue !== undefined);
    const byRevenue = rows.slice().sort((a, b) => (b.revenue || 0) - (a.revenue || 0));
    const rank = new Map(byRevenue.map((r, i) => [r.store_key, i + 1]));
    const maxRevenue = byRevenue.length ? byRevenue[0].revenue || 1 : 1;

    const { key, dir } = state.sort;
    const sign = dir === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      const av = a[key], bv = b[key];
      if (key === "store_name") return sign * String(av).localeCompare(String(bv));
      return sign * ((av ?? -Infinity) - (bv ?? -Infinity));
    });

    const table = $("stores-table");
    table.tHead.replaceChildren(h("tr", {}, STORE_COLUMNS.map((col) => {
      const active = col.key === key;
      const th = h("th", {
        scope: "col",
        class: [col.type === "num" ? "num" : "", col.cls || ""].join(" ").trim() || null,
        "aria-sort": active ? (dir === "asc" ? "ascending" : "descending") : null,
        title: col.title || null,
      });
      if (col.type === "none") { th.textContent = col.label; th.style.textAlign = "right"; return th; }
      const arrow = s("svg", { class: "arrow", viewBox: "0 0 10 10", "aria-hidden": "true" },
        s("path", { d: active && dir === "asc" ? "M5 2 9 7H1z" : "M5 8 1 3h8z", fill: "currentColor" }));
      th.append(h("button", {
        type: "button", class: "sort-btn",
        onclick: () => {
          state.sort = active ? { key, dir: dir === "asc" ? "desc" : "asc" } : { key: col.key, dir: col.type === "text" ? "asc" : "desc" };
          renderStores();
          table.tHead.querySelector(`[data-key="${col.key}"]`)?.focus();
        },
        "data-key": col.key,
      }, col.label, arrow));
      return th;
    })));

    table.tBodies[0].replaceChildren(...rows.map((r) => h("tr", {},
      h("th", { scope: "row" }, h("div", { class: "store-cell" },
        h("span", { class: "rank", title: "Revenue rank", text: String(rank.get(r.store_key)) }),
        h("span", { class: "store-name" }, r.store_name.replace(/^Dukaan\s+/, ""),
          h("span", { class: "store-meta", text: `${r.city} · ${r.store_type}` })))),
      h("td", { class: "num" }, h("span", { class: "rev-cell" },
        h("span", { text: usd(r.revenue) }),
        h("span", { class: "rev-bar", style: `width:${Math.max(2, Math.round((r.revenue / maxRevenue) * 64))}px`, "aria-hidden": "true" }))),
      h("td", { class: "num", text: pct(r.margin_pct) }),
      h("td", { class: "num col-hide-sm", text: usd2(r.avg_order_value) }),
      h("td", { class: "num col-hide-sm", text: int(r.completed_orders) }),
      h("td", { class: "num col-hide-xs", text: pct(r.returned_rate) }),
      h("td", {}, sparkline(r.sparkline || [], months, state.year)))));
  }

  // ------------------------------------------------------------------ categories
  function renderCategories() {
    const items = (state.data.categories.periods[state.year] || []).slice().sort((a, b) => (b.revenue || 0) - (a.revenue || 0));
    const max = items.length ? items[0].revenue || 1 : 1;
    $("categories").replaceChildren(...items.map((c) => {
      const unknown = c.category === "Unknown";
      const name = unknown ? "Unknown product" : c.category;
      const share = pct(c.revenue_share);
      const lines = [name, `${usd2(c.revenue)} revenue (${share} of total)`,
        unknown ? "Margin unknown: product missing from the catalogue" : `${pct(c.margin_pct)} gross margin`,
        `${int(c.units_sold)} units sold`];
      const row = h("li", { class: "bar-row" },
        h("div", { class: "bar-label", title: name }, name,
          h("span", { class: "store-meta", text: unknown ? "margin n/a" : `${pct(c.margin_pct)} margin` })),
        h("div", { class: "bar-track" },
          h("span", { class: `bar-fill${unknown ? " is-unknown" : ""}`, style: `width:calc((100% - 118px) * ${(c.revenue || 0) / max})` }),
          h("span", { class: "bar-value" }, h("strong", { text: usdCompact(c.revenue) }), ` ${share}`)));
      attachTip(row, lines);
      return row;
    }));
  }

  // ------------------------------------------------------------------ channel x payment
  function paymentSlots() {
    const known = state.data.channels.payment_methods || [];
    const ordered = PAYMENT_ORDER.filter((p) => known.includes(p)).concat(known.filter((p) => !PAYMENT_ORDER.includes(p)));
    return ordered.map((p, i) => ({ id: p, slot: i < 4 ? i + 1 : null }));
  }

  function renderMix() {
    const slots = paymentSlots();
    const colour = (slot) => (slot ? `var(--pay-${slot})` : "var(--unknown)");
    $("mix-legend").replaceChildren(...slots.map(({ id, slot }) =>
      h("li", {}, h("span", { class: "key-swatch", style: `background:${colour(slot)}` }), paymentName(id))));

    const cells = state.data.channels.periods[state.year] || [];
    const total = cells.reduce((sum, c) => sum + (c.revenue || 0), 0) || 1;
    const channels = Array.from(new Set(cells.map((c) => c.channel))).map((channel) => {
      const parts = cells.filter((c) => c.channel === channel);
      return { channel, parts, revenue: parts.reduce((sum, c) => sum + (c.revenue || 0), 0) };
    }).sort((a, b) => b.revenue - a.revenue);

    $("mix").replaceChildren(...channels.map(({ channel, parts, revenue }) => {
      const bar = h("div", { class: "mix-bar" });
      slots.forEach(({ id, slot }) => {
        const part = parts.find((p) => p.payment_method === id);
        if (!part || !part.revenue) return;
        const share = part.revenue / (revenue || 1);
        const seg = h("div", { class: "mix-seg", "data-slot": slot || "x", style: `flex:${part.revenue} 1 0;background:${colour(slot)}` },
          h("span", { class: "seg-label", "aria-hidden": "true", text: F.pct0.format(share) }));
        attachTip(seg, [`${paymentName(id)} · ${channelName(channel)}`, `${usd2(part.revenue)} (${pct(share)} of channel revenue)`, `${int(part.completed_orders)} completed orders`]);
        bar.append(seg);
      });
      return h("div", { class: "mix-row" },
        h("div", { class: "mix-head" }, h("strong", { text: channelName(channel) }),
          h("span", { text: `${usdCompact(revenue)} · ${pct(revenue / total)} of revenue` })),
        bar);
    }));
    requestAnimationFrame(fitSegmentLabels);
  }

  /** A segment label that doesn't fit with padding is hidden; the tooltip and legend carry it. */
  function fitSegmentLabels() {
    document.querySelectorAll(".mix-seg").forEach((seg) => {
      const label = seg.querySelector(".seg-label");
      if (!label) return;
      label.style.visibility = "visible";
      if (label.offsetWidth + 10 > seg.clientWidth) label.style.visibility = "hidden";
    });
  }

  // ------------------------------------------------------------------ quality
  const QUALITY_ICONS = {
    copy: "M8 8h11v11H8zM5 16V5h11",
    merge: "M6 4v5a5 5 0 0 0 5 5h7M15 11l3 3-3 3M6 20v-4",
    user: "M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM4 21a8 8 0 0 1 16 0",
    box: "M3 7l9-4 9 4-9 4-9-4ZM3 7v10l9 4 9-4V7M12 11v10",
    zero: "M12 4c-3.5 0-5 3.5-5 8s1.5 8 5 8 5-3.5 5-8-1.5-8-5-8ZM8 18 16 6",
    mail: "M3 6h18v12H3zM3 7l9 6 9-6",
  };

  function renderQuality() {
    const q = state.data.quality || {};
    const share = (n, d) => (d ? pct(n / d) : dash);
    const tiles = [];
    if (q.duplicate_customers_removed) {
      const t = q.duplicate_customers_removed;
      tiles.push({ icon: "copy", value: int(t.count), label: "duplicate customer rows removed",
        detail: `${int(t.raw_rows)} raw rows became ${int(t.clean_rows)} unique customers.`, model: "stg_customers" });
    }
    if (q.resent_orders_collapsed) {
      const t = q.resent_orders_collapsed;
      tiles.push({ icon: "merge", value: int(t.count), label: "re-sent orders collapsed",
        detail: `${int(t.raw_rows)} raw rows became ${int(t.clean_rows)} orders; the latest version of each wins.`, model: "stg_orders" });
    }
    if (q.walk_in_orders) {
      const t = q.walk_in_orders;
      tiles.push({ icon: "user", value: int(t.count), label: "walk-in orders",
        detail: `${pct(t.share)} of orders have no customer; they map to the Walk-in member (customer_key = -1).`, model: "dim_customer" });
    }
    if (q.orphan_product_lines) {
      const t = q.orphan_product_lines;
      tiles.push({ icon: "box", value: int(t.count), label: "order lines with an unknown product",
        detail: `${int(t.product_ids)} product ids missing from the catalogue. ${usd2(t.revenue)} of revenue kept under Unknown product, with margin left null.`, model: "fct_sales" });
    }
    if (q.blank_discounts_zeroed) {
      const t = q.blank_discounts_zeroed;
      const lines = q.orphan_product_lines ? q.orphan_product_lines.order_lines : null;
      tiles.push({ icon: "zero", value: int(t.count), label: "blank discounts read as $0",
        detail: lines ? `${share(t.count, lines)} of order lines had no discount value.` : "Empty discount fields mean no discount.", model: "stg_order_items" });
    }
    if (q.emails_normalised) {
      const t = q.emails_normalised;
      tiles.push({ icon: "mail", value: int(t.count), label: "emails trimmed and lower-cased",
        detail: `Plus ${int(t.blank_to_null)} blank emails stored as null.`, model: "stg_customers" });
    }
    $("quality").replaceChildren(...tiles.map((t) => h("article", { class: "q-tile" },
      h("div", { class: "q-icon", "aria-hidden": "true" },
        s("svg", { viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", "stroke-width": "2", "stroke-linecap": "round", "stroke-linejoin": "round" },
          s("path", { d: QUALITY_ICONS[t.icon] }))),
      h("div", { class: "q-body" },
        h("p", { class: "q-value", style: "margin:0", text: t.value }),
        h("h3", { class: "q-label", style: "margin:0", text: t.label }),
        h("p", { class: "q-detail", text: t.detail }),
        h("p", { class: "q-model", style: "margin-bottom:0" }, "Fixed in ", h("code", { text: t.model }))))));
  }

  // ------------------------------------------------------------------ orchestration
  function renderFiltered() {
    $("filter-note").textContent = state.year === ALL
      ? "Revenue counts completed orders only. Amounts in USD."
      : `Showing ${state.year}${state.data.summary.periods[String(Number(state.year) - 1)] ? `, compared with ${Number(state.year) - 1}` : ""}. Revenue counts completed orders only.`;
    renderKpis();
    renderMonthly();
    renderStores();
    renderCategories();
    renderMix();
  }

  async function init() {
    setupTheme();
    try {
      const values = await Promise.all(DATA_FILES.map(loadJSON));
      state.data = Object.fromEntries(DATA_FILES.map((name, i) => [name, values[i]]));
      setupFilters();
      renderBuildLine();
      renderMonthlyTable();
      renderQuality();
      renderFiltered();
    } catch (error) {
      console.error(error);
      showError(error);
      return;
    }
    const main = $("main");
    main.classList.remove("is-loading");
    main.setAttribute("aria-busy", "false");

    let resizeTimer;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(fitSegmentLabels, 120);
    });
    window.addEventListener("scroll", hideTip, { passive: true });
  }

  init();
})();
