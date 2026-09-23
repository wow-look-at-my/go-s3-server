// The dashboard polls a single JSON snapshot and redraws. It keeps no state
// on the server: a rate is the difference between samples this page took, so
// a reload starts the rate over and nothing else.

const POLL_MS = 1000;

const state = {
	previous: null, // the last snapshot, for rates
	peak: 0, // the highest rate this page has seen
	timer: null,
	projects: null, // the last per-project totals, for per-project rates
	projectNames: null,
};

const $ = (id) => document.getElementById(id);

// --- reading the snapshot ---------------------------------------------------

// A counter that has not fired yet is absent from the registry, and "absent"
// and "empty" mean the same thing on this page.
function value(stats, name) {
	const m = stats.metrics[name];
	if (!m) return 0;
	if (typeof m.value === "number") return m.value;
	return sum(m.series);
}

function series(stats, name) {
	const m = stats.metrics[name];
	return m && m.series ? m.series : {};
}

function sum(obj) {
	return Object.values(obj || {}).reduce((a, b) => a + b, 0);
}

// --- formatting -------------------------------------------------------------

function bytes(n) {
	if (!n) return "0 B";
	const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
	let i = 0;
	while (n >= 1024 && i < units.length - 1) {
		n /= 1024;
		i++;
	}
	return `${n < 10 && i > 0 ? n.toFixed(1) : Math.round(n)} ${units[i]}`;
}

function count(n) {
	return Math.round(n).toLocaleString();
}

function percent(part, whole) {
	if (!whole) return "--";
	return `${((part / whole) * 100).toFixed(1)}%`;
}

function duration(seconds) {
	const s = Math.floor(seconds);
	const d = Math.floor(s / 86400);
	const h = Math.floor((s % 86400) / 3600);
	const m = Math.floor((s % 3600) / 60);
	if (d) return `up ${d}d ${h}h`;
	if (h) return `up ${h}h ${m}m`;
	if (m) return `up ${m}m`;
	return `up ${s}s`;
}

// --- drawing ----------------------------------------------------------------

function tile(label, value, sub, tone) {
	const el = document.createElement("div");
	el.className = tone ? `tile ${tone}` : "tile";
	el.innerHTML = `<div class="label"></div><div class="value"></div><div class="sub"></div>`;
	el.querySelector(".label").textContent = label;
	el.querySelector(".value").textContent = value;
	el.querySelector(".sub").textContent = sub || "";
	return el;
}

// A single bar: a label row over a <scratch-progress>. The component owns the
// geometry and the fill colour, so tone is any of its own states rather than a
// class this page styles.
function bar(name, n, max, tone, right) {
	const el = document.createElement("div");
	el.className = "bar";
	el.innerHTML = `<span class="name"></span><span class="n"></span><scratch-progress></scratch-progress>`;
	el.querySelector(".name").textContent = name;
	el.querySelector(".n").textContent = right !== undefined ? right : count(n);
	const meter = el.querySelector("scratch-progress");
	meter.setAttribute("max", String(max > 0 ? max : 1));
	meter.setAttribute("value", String(max > 0 ? Math.min(n, max) : 0));
	meter.setAttribute("state", tone === "good" ? "signal" : tone === "bad" ? "danger" : "accent");
	return el;
}

function para(text) {
	const p = document.createElement("p");
	p.className = "muted";
	p.textContent = text;
	return p;
}

function fill(container, nodes) {
	container.replaceChildren(...nodes);
}

function rows(table, entries) {
	const body = table.querySelector("tbody");
	body.replaceChildren(
		...entries.map(([label, text, alert]) => {
			const tr = document.createElement("tr");
			const td1 = document.createElement("td");
			td1.textContent = label;
			const td2 = document.createElement("td");
			td2.className = alert ? "n alert" : "n";
			td2.textContent = text;
			tr.append(td1, td2);
			return tr;
		}),
	);
}

// --- panels -----------------------------------------------------------------

// A server holding keys must not report "0 B": say the number is not measured
// yet instead of drawing a confident empty.
function cacheSizeTile(cacheBytes, budget, indexed) {
	if (cacheBytes === 0 && indexed > 0) {
		return tile("cache size", "not measured", "measured at the first eviction sweep");
	}
	const sub = budget ? `${percent(cacheBytes, budget)} of ${bytes(budget)}` : "no size budget";
	return tile("cache size", bytes(cacheBytes), sub, budget && cacheBytes > budget * 0.9 ? "warn" : "");
}

// The header state: an LED plus its word. The LED carries each states the
// design language defines, so serving is good, draining is accent, and a poll
// that failed is bad.
function setState(text, led) {
	$("state-text").textContent = text;
	const el = $("state-led");
	el.setAttribute("state", led);
	el.toggleAttribute("live", led !== "bad");
}

// hitRateTile reports the batch path, which is where every read of the current
// client goes; a single-object GET is an older client, and it counts only when
// there is no batch traffic to report.
function hitRateTile(stats) {
	const kinds = series(stats, "s3_batch_keys_total");
	const requested = kinds.requested || 0;
	if (requested) {
		const found = kinds.found || 0;
		return tile("hit rate", percent(found, requested), `${count(found)} of ${count(requested)} keys asked for in batches`);
	}
	const outcomes = series(stats, "s3_get_requests_total");
	const reads = sum(outcomes);
	const hits = outcomes.hit || 0;
	return tile("hit rate", reads ? percent(hits, reads) : "--", `${count(hits)} of ${count(reads)} single GETs`);
}

function drawTiles(stats) {
	const cacheBytes = value(stats, "s3_cache_bytes");
	const budget = stats.eviction.max_bytes;
	const inFlight = value(stats, "cache_http_in_flight_requests");
	const admitted = value(stats, "cache_http_admitted_requests");
	const limit = stats.server.max_concurrent_requests;
	const indexed = value(stats, "s3_index_hashes") + value(stats, "s3_index_pending_hashes");
	const rejected = value(stats, "s3_http_rejected_total");
	const shed = rejected ? `${count(rejected)} shed with 503` : "nothing shed";

	fill($("tiles"), [
		hitRateTile(stats),
		cacheSizeTile(cacheBytes, budget, indexed),
		tile("keys advertised", count(indexed), "action hashes in /_index"),
		tile("slots in use", `${count(admitted)} / ${count(limit)}`, `${count(inFlight)} in flight; ${shed}`, rejected ? "warn" : ""),
		tile("batch requests", count(value(stats, "s3_batch_requests_total")), `${count(series(stats, "s3_batch_keys_total").streamed || 0)} bodies streamed`),
		tile("evicted", count(value(stats, "s3_evictions_total")), `${bytes(value(stats, "s3_evicted_bytes_total"))} reclaimed`),
	]);
}

function drawReads(stats) {
	const outcomes = series(stats, "s3_get_requests_total");
	const total = sum(outcomes) || 1;
	const tone = (k) => (k === "hit" ? "good" : k === "miss_not_found" ? "" : "bad");
	fill(
		$("get-outcomes"),
		Object.entries(outcomes)
			.sort((a, b) => b[1] - a[1])
			.map(([k, v]) => bar(k, v, total, tone(k), `${count(v)}  ${percent(v, total)}`)),
	);
	$("single-gets").hidden = !Object.keys(outcomes).length;

	const kinds = series(stats, "s3_batch_keys_total");
	const requested = kinds.requested || 0;
	const scale = Math.max(requested, ...Object.values(kinds), 1);
	fill(
		$("batch-kinds"),
		["requested", "found", "prefetched", "client_held", "streamed"].map((k) =>
			bar(k, kinds[k] || 0, scale, k === "found" ? "good" : "", k === "found" && requested ? `${count(kinds[k] || 0)}  ${percent(kinds[k] || 0, requested)}` : undefined),
		),
	);
}

function drawTraffic(stats) {
	const byRoute = {};
	for (const [key, v] of Object.entries(series(stats, "cache_http_requests_total"))) {
		const route = (key.split(",").find((p) => p.startsWith("route=")) || "route=?").slice(6);
		byRoute[route] = (byRoute[route] || 0) + v;
	}
	const entries = Object.entries(byRoute).sort((a, b) => b[1] - a[1]);
	const max = entries.length ? entries[0][1] : 1;
	fill(
		$("routes"),
		entries.length ? entries.map(([k, v]) => bar(k, v, max)) : [para("no requests yet")],
	);
	drawRate(stats);
}

// push feeds a single sample to a <perf-graph>. The element is defined by a
// module fetched at run time, so an early poll can land before it upgrades; a
// plain element has no push and the sample is dropped rather than throwing.
function push(id, v) {
	const el = $(id);
	if (el && typeof el.push === "function") el.push(v);
}

// The graphs are gauges over time, so each a single takes the value as it
// stands. The rate is the exception: the server reports a counter, and a
// rate is the difference between samples this page took.
function drawRate(stats) {
	const total = sum(series(stats, "cache_http_requests_total"));
	const prev = state.previous;
	state.previous = { total, generated_at: stats.generated_at };
	if (!prev) return;

	const dt = (new Date(stats.generated_at) - new Date(prev.generated_at)) / 1000;
	const dv = total - prev.total;
	// A counter that went backwards means the server restarted between
	// samples. Drop the point rather than draw a negative rate.
	if (dv < 0) {
		state.peak = 0;
		$("rate-chart").clear?.();
		return;
	}
	if (dt <= 0) return;

	// perf-graph owns the history, the scale and the redraw. It is fed the
	// newest sample and nothing else.
	const now = dv / dt;
	state.peak = Math.max(state.peak, now);
	push("rate-chart", now);
	$("rate-label").textContent = `${now.toFixed(1)} req/s now, peak ${state.peak.toFixed(1)} since this page loaded`;
}

// Both gauges beside the rate. Both read straight off the snapshot, so
// neither needs a previous sample and both start drawing on the earliest
// poll. The batch series is the same a single hitRateTile reads, for the same reason.
function drawGauges(stats) {
	const kinds = series(stats, "s3_batch_keys_total");
	const requested = kinds.requested || 0;
	if (requested) $("hit-chart").push(((kinds.found || 0) / requested) * 100);
	$("inflight-chart").push(value(stats, "cache_http_in_flight_requests"));
}

// --- by project ---------------------------------------------------------------

// s3_project_objects_total is labelled (project, kind), which the snapshot
// flattens into series keys spelled "kind=hit,project=go-toolchain" — sorted,
// so the order of the parts is fixed.
function labelled(key) {
	const out = {};
	for (const part of key.split(",")) {
		const eq = part.indexOf("=");
		if (eq > 0) out[part.slice(0, eq)] = part.slice(eq + 1);
	}
	return out;
}

// The stacked chart is a RATE, so it needs samples, the same as the overall
// request rate. Counters only ever rise, so a drop means the server
// restarted and the history is about a different process.
function drawProjects(stats) {
	const totals = {};
	for (const [key, v] of Object.entries(series(stats, "s3_project_objects_total"))) {
		const { project, kind } = labelled(key);
		if (!project) continue;
		const row = (totals[project] ||= { hit: 0, lookahead: 0, put: 0, miss: 0 });
		if (kind in row) row[kind] += v;
	}

	drawProjectMisses(totals);

	const prev = state.projects;
	state.projects = { totals, at: stats.generated_at };
	const chart = $("project-chart");
	if (!chart) return;
	// The chart comes from the js-snippets library site at run time, so this
	// page can be newer than the component it loaded. Say that on the page:
	// a stacked chart that silently stays blank reads as "no traffic".
	if (typeof chart.pushSeries !== "function") {
		$("project-label").textContent =
			"the loaded <perf-graph> has no stacked-area support, so this chart stays blank until the library site is republished";
		return;
	}

	// Names in a fixed order, so a band keeps its place and its color as
	// projects come and go. Sorted by name, not by traffic: a band that
	// reorders itself every poll is unreadable.
	const names = Object.keys(totals).sort();
	if (!state.projectNames || state.projectNames.join("\u0000") !== names.join("\u0000")) {
		state.projectNames = names;
		chart.series = names.map((key) => ({ key }));
	}

	if (!prev) return;
	const dt = (new Date(stats.generated_at) - new Date(prev.at)) / 1000;
	if (dt <= 0) return;
	const rates = {};
	let total = 0;
	for (const name of names) {
		const was = prev.totals[name];
		const now = totals[name];
		const moved = now.hit + now.lookahead + now.put + now.miss - (was ? was.hit + was.lookahead + was.put + was.miss : 0);
		if (moved < 0) {
			// The server restarted between samples. Start the history over
			// rather than draw a negative band.
			chart.clear();
			return;
		}
		rates[name] = moved / dt;
		total += rates[name];
	}
	chart.pushSeries(rates);
	$("project-label").textContent = names.length
		? `${total.toFixed(1)} objects/s across ${names.length} project${names.length === 1 ? "" : "s"}`
		: "no project has moved an object yet";
}

// The miss rate is what the chart cannot show: a project can be a thin band
// and still be missing almost everything it asks for.
function drawProjectMisses(totals) {
	const entries = Object.entries(totals)
		.map(([name, row]) => {
			const asked = row.hit + row.miss;
			return [name, row, asked];
		})
		.filter(([, , asked]) => asked > 0)
		.sort((a, b) => b[1].miss / b[2] - a[1].miss / a[2]);
	if (entries.length === 0) {
		rows($("project-misses"), [["no project has asked for a key yet", "--", false]]);
		return;
	}
	rows(
		$("project-misses"),
		entries.map(([name, row, asked]) => [
			name,
			`${percent(row.miss, asked)} of ${count(asked)}`,
			row.miss / asked > 0.5,
		]),
	);
}

// Every entry here is a number that should be empty, or should be falling.
// The alert flag turns the value red so a rising a single is visible without
// reading the labels.
function drawTripwires(stats) {
	const outcomes = series(stats, "s3_get_requests_total");
	const unservable = outcomes.miss_advertised_unservable || 0;
	const entries = [
		["advertised but unservable GETs", count(unservable), unservable > 0],
		["self-heal failures (key de-advertised)", count(value(stats, "s3_self_heal_failures_total")), value(stats, "s3_self_heal_failures_total") > 0],
		["outputid mismatches repaired", count(value(stats, "s3_outputid_mismatch_total")), value(stats, "s3_outputid_mismatch_total") > 0],
		["self-heal repairs (one-time per object)", count(value(stats, "s3_self_heal_repairs_total")), false],
		["module indexes refused on PUT", count(sum(series(stats, "s3_put_refusals_total"))), false],
		["module indexes evicted on read", count(value(stats, "s3_module_index_evictions_total")), false],
		["requests shed at capacity", count(value(stats, "s3_http_rejected_total")), value(stats, "s3_http_rejected_total") > 0],
		["auth failures", count(value(stats, "cache_auth_failures_total")), false],
		["metadata xattrs dropped", count(value(stats, "s3_metadata_xattrs_dropped_total")), value(stats, "s3_metadata_xattrs_dropped_total") > 0],
		["deprecated S3 requests", count(sum(series(stats, "s3_deprecated_requests_total"))), false],
		["memory-pressure shrinks", count(value(stats, "s3_memory_shrinks_total")), false],
	];
	rows($("tripwires"), entries);
}

function drawMemory(stats) {
	const limit = value(stats, "s3_memory_limit_bytes");
	const inUse = value(stats, "s3_memory_in_use_bytes");
	const nodes = [];
	if (limit > 0) {
		nodes.push(bar("process in use", inUse, limit, inUse > limit * 0.85 ? "warn" : "good", `${bytes(inUse)} of ${bytes(limit)}`));
	} else {
		nodes.push(para("no process memory limit discovered, so caches use fixed default budgets"));
	}
	const held = series(stats, "s3_cache_memory_bytes");
	const budgets = series(stats, "s3_cache_memory_budget_bytes");
	for (const [name, v] of Object.entries(held).sort()) {
		const budget = budgets[name] || 0;
		nodes.push(bar(`cache: ${name}`, v, budget, "", `${bytes(v)} of ${bytes(budget)}`));
	}
	const hits = value(stats, "s3_meta_cache_hits_total");
	const misses = value(stats, "s3_meta_cache_misses_total");
	if (hits + misses > 0) {
		nodes.push(bar("metadata cache hit ratio", hits, hits + misses, "good", percent(hits, hits + misses)));
	}
	fill($("memory"), nodes);
}

function drawConfig(stats) {
	const s = stats.server;
	const e = stats.eviction;
	rows($("config"), [
		["bucket", s.bucket],
		["data dir", s.data_dir],
		["cache API", s.listen],
		["dashboard", s.dashboard_listen],
		["metrics", s.metrics_listen || "off"],
		["authentication", s.auth_disabled ? "DISABLED on the cache API" : "basic auth on the cache API", s.auth_disabled],
		["write_once", `${s.write_once_action} / notify ${s.write_once_notification}`],
		["max concurrent requests", count(s.max_concurrent_requests)],
		["max object bytes", bytes(s.max_object_bytes)],
		["eviction", e.enabled ? `max ${bytes(e.max_bytes)}, age ${e.max_age}, every ${e.interval}` : "DISABLED: the cache grows until the disk fills", !e.enabled],
		["index entries", count(value(stats, "s3_index_entries"))],
		["index rebuilds", count(value(stats, "s3_index_rebuild_duration_seconds_count"))],
	]);
}

function draw(stats) {
	$("bucket").textContent = stats.server.bucket;
	$("uptime").textContent = duration(stats.uptime_seconds);
	setState(stats.server.draining ? "draining" : "serving", stats.server.draining ? "accent" : "good");
	$("footer-note").textContent = `updated ${new Date(stats.generated_at).toLocaleTimeString()}. `;

	drawTiles(stats);
	drawReads(stats);
	drawTraffic(stats);
	drawGauges(stats);
	drawProjects(stats);
	drawTripwires(stats);
	drawMemory(stats);
	drawConfig(stats);
}

// --- bandwidth chart --------------------------------------------------------

// The bandwidth chart is drawn on the page's own canvas rather than by another
// <perf-graph>: that element draws ONE scalar series, and this chart is a stack
// of them, one per module, with the index fetches as one more. Nothing is
// imported for it.

// One alpha per module band over the accent the page already uses. A stack does
// not overlap, so an alpha ramp reads as a value ramp and the whole chart stays
// in the one accent the design language reserves for "this is the thing you are
// looking at". The index series is not a module, so it takes the neutral.
const BAND_ALPHA = [0.95, 0.76, 0.6, 0.47, 0.37];
const OTHER_ALPHA = 0.2;
const INDEX_ALPHA = 0.6;

const bandwidth = { series: null };

// token reads one of the design language's custom properties off an element.
// The value is handed to the canvas as it stands, so a token that is not a
// literal colour is checked first: a canvas ignores a colour it cannot parse,
// which would leave the plot painted in whatever was set last.
function token(ctx, el, name, fallback) {
	const value = getComputedStyle(el).getPropertyValue(name).trim();
	if (!value) return fallback;
	const previous = ctx.fillStyle;
	ctx.fillStyle = fallback;
	ctx.fillStyle = value;
	const resolved = ctx.fillStyle;
	ctx.fillStyle = previous;
	return resolved === fallback ? fallback : resolved;
}

// The last band the endpoint ranks is always the remainder, so the chart can
// tell it apart from a named module without knowing what the server calls it.
function bandAlpha(bands, index) {
	return index === bands.length - 1 ? OTHER_ALPHA : BAND_ALPHA[index % BAND_ALPHA.length];
}

// swatch is the colour the canvas drew a band in, as a stylesheet value: a
// canvas has to be handed a real colour, and a legend swatch does not, so the
// mix is left to the browser, over the same token at the same alpha.
function swatch(anchor, alpha) {
	return `color-mix(in srgb, var(${anchor}) ${Math.round(alpha * 100)}%, transparent)`;
}

function legendItem(colour, label, n) {
	const item = document.createElement("span");
	item.className = "item";
	const dot = document.createElement("span");
	dot.className = "swatch";
	dot.style.background = colour;
	const name = document.createElement("span");
	name.textContent = label;
	const value = document.createElement("span");
	value.className = "n";
	value.textContent = bytes(n);
	item.append(dot, name, value);
	return item;
}

// drawBandwidth stacks the bands the endpoint ranked, with the index above
// them, over an axis scaled to the busiest second in the window. Because the
// bands and the index add up to the total, the top of the stack IS the total
// bandwidth the axis stands for.
function drawBandwidth() {
	const canvas = $("bandwidth-chart");
	// Named around the module-level series() helper: this is the whole window,
	// not one of the snapshot's labeled series.
	const chart = bandwidth.series;
	if (!canvas || !chart || !chart.points) return;
	const ctx = canvas.getContext("2d");
	if (!ctx) return;

	const dpr = typeof devicePixelRatio === "number" && devicePixelRatio > 0 ? devicePixelRatio : 1;
	const cssW = canvas.clientWidth || 640;
	const cssH = canvas.clientHeight || 140;
	canvas.width = Math.max(1, Math.round(cssW * dpr));
	canvas.height = Math.max(1, Math.round(cssH * dpr));
	ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
	ctx.clearRect(0, 0, cssW, cssH);

	const points = chart.points;
	const bands = chart.bands || [];
	const grid = token(ctx, canvas, "--border-light", "#1e222c");
	const label = token(ctx, canvas, "--text-muted", "#6b7280");
	const strong = token(ctx, canvas, "--text-bright", "#e8ecf4");
	const accent = token(ctx, canvas, "--accent", "#ffae00");
	ctx.font = "10px " + token(ctx, canvas, "--font-mono", "monospace");

	let busiest = 0;
	for (const point of points) if (point.total > busiest) busiest = point.total;

	const plotTop = 2;
	const plotBottom = cssH - 2;
	const scale = busiest > 0 ? (plotBottom - plotTop) / busiest : 0;
	const barWidth = points.length ? cssW / points.length : cssW;

	// The axis: the top of the scale and its middle, ruled and labelled. The top
	// is the total bandwidth the stack reaches, which is what the chart is read
	// against; the baseline is zero and needs no label.
	ctx.textAlign = "right";
	ctx.textBaseline = "bottom";
	for (const [level, colour] of [[0, strong], [0.5, label]]) {
		const y = Math.round(plotTop + level * (plotBottom - plotTop)) + 0.5;
		ctx.strokeStyle = grid;
		ctx.beginPath();
		ctx.moveTo(0, y);
		ctx.lineTo(cssW, y);
		ctx.stroke();
		ctx.fillStyle = colour;
		ctx.fillText(bytes(busiest * (1 - level)) + "/s", cssW - 3, y - 1);
	}
	ctx.strokeStyle = grid;
	ctx.beginPath();
	ctx.moveTo(0, plotBottom + 0.5);
	ctx.lineTo(cssW, plotBottom + 0.5);
	ctx.stroke();

	// The stack. A band with bytes in it is drawn at least a device pixel tall,
	// so a module that served anything at all is on the chart however small its
	// share, and a band with nothing in it is drawn nowhere.
	const totals = new Map();
	points.forEach((point, i) => {
		const x = i * barWidth;
		let top = plotBottom;
		bands.forEach((band, b) => {
			const n = (point.modules && point.modules[band]) || 0;
			if (!n) return;
			totals.set(band, (totals.get(band) || 0) + n);
			const h = Math.max(1 / dpr, n * scale);
			ctx.globalAlpha = bandAlpha(bands, b);
			ctx.fillStyle = accent;
			ctx.fillRect(x, top - h, barWidth, h);
			top -= h;
		});
		if (point.index) {
			totals.set("index", (totals.get("index") || 0) + point.index);
			const h = Math.max(1 / dpr, point.index * scale);
			ctx.globalAlpha = INDEX_ALPHA;
			ctx.fillStyle = label;
			ctx.fillRect(x, top - h, barWidth, h);
		}
	});
	ctx.globalAlpha = 1;

	// The key, in the stack's own order, carrying what each band served over the
	// whole window: the same ranking the endpoint cut the top modules by.
	let served = 0;
	for (const point of points) served += point.total;
	const legend = bands.map((band, b) => legendItem(swatch("--accent", bandAlpha(bands, b)), band, totals.get(band) || 0));
	legend.push(legendItem(swatch("--text-muted", INDEX_ALPHA), "index fetches", totals.get("index") || 0));
	fill($("bandwidth-legend"), legend);

	// The numbers the shape stands for, so the chart is not the only way to read
	// the window it covers.
	$("bandwidth-note").textContent = `peak ${bytes(busiest)}/s, ${bytes(served)} over the last ${count(chart.retention_seconds)}s, in ${count(chart.bucket_seconds)}s buckets`;
}

// The bandwidth series has its own endpoint, and so its own failure: a chart
// that cannot be drawn must not blank the tiles that could.
async function pollBandwidth() {
	try {
		const res = await fetch("/api/bandwidth", { cache: "no-store" });
		if (!res.ok) throw new Error(`bandwidth endpoint answered ${res.status}`);
		bandwidth.series = await res.json();
		drawBandwidth();
	} catch (err) {
		$("bandwidth-note").textContent = `bandwidth unavailable: ${err.message}`;
	}
}

// --- polling ----------------------------------------------------------------

async function poll() {
	try {
		const res = await fetch("/api/stats", { cache: "no-store" });
		if (!res.ok) throw new Error(`stats endpoint answered ${res.status}`);
		draw(await res.json());
	} catch (err) {
		// A failed poll is reported where the connection state already is. The
		// page keeps the numbers it drew last, and they are stamped with the
		// time they came from.
		setState("unreachable", "bad");
		$("footer-note").textContent = `last poll failed: ${err.message}. `;
	}
	// The bandwidth series comes from its own endpoint, so it is polled on its
	// own terms: its failure is a note under its own chart, never the state of a
	// page whose numbers all arrived.
	await pollBandwidth();
}

// The toggle is a custom element whose module is deferred, and this script is
// a classic a single at the end of the body, so it runs earliest. Until the
// element upgrades it carries the attribute and no property, and reading the
// property alone reports "off" and stops the page polling at all.
function live() {
	const el = $("autorefresh");
	return typeof el.checked === "boolean" ? el.checked : el.hasAttribute("checked");
}

function schedule() {
	clearInterval(state.timer);
	if (live()) state.timer = setInterval(poll, POLL_MS);
}

$("autorefresh").addEventListener("change", () => {
	schedule();
	if (live()) poll();
});

// `checked` is a property scratch-toggle only has a single time it is
// upgraded. Read it before that and the answer is undefined, which reads as
// "live is off": the page then draws a single snapshot and never polls again.
// Waiting makes the poll loop independent of which script the browser ran earliest.
poll();
await customElements.whenDefined("scratch-toggle");
schedule();
// A single time the element upgrades its property is authoritative; re-read
// it in case it disagrees with the attribute this started on.
customElements.whenDefined("scratch-toggle").then(schedule);
