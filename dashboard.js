// The dashboard polls one JSON snapshot and redraws. It keeps no state on the
// server: a rate is the difference between two samples this page took, so a
// reload starts the rate over and nothing else.

const POLL_MS = 1000;

const state = {
	previous: null, // the last snapshot, for rates
	peak: 0, // the highest rate this page has seen
	timer: null,
};

const $ = (id) => document.getElementById(id);

// --- reading the snapshot ---------------------------------------------------

// value returns an unlabeled metric, or 0 when the server has never touched it.
// A counter that has not fired yet is absent from the registry, and "absent"
// and "zero" mean the same thing on this page.
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

// One bar: a label row over a <scratch-progress>. The component owns the
// geometry and the fill colour, so tone is one of its own states rather than a
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

// The server measures cache size during an eviction sweep, so before the first
// sweep the gauge is 0. A server holding keys must not report "0 B": say the
// number is not measured yet instead of drawing a confident zero.
function cacheSizeTile(cacheBytes, budget, indexed) {
	if (cacheBytes === 0 && indexed > 0) {
		return tile("cache size", "not measured", "measured at the first eviction sweep");
	}
	const sub = budget ? `${percent(cacheBytes, budget)} of ${bytes(budget)}` : "no size budget";
	return tile("cache size", bytes(cacheBytes), sub, budget && cacheBytes > budget * 0.9 ? "warn" : "");
}

// The header state: an LED plus its word. The LED carries the three states the
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
	const limit = stats.server.max_concurrent_requests;
	const indexed = value(stats, "s3_index_hashes") + value(stats, "s3_index_pending_hashes");
	const rejected = value(stats, "s3_http_rejected_total");

	fill($("tiles"), [
		hitRateTile(stats),
		cacheSizeTile(cacheBytes, budget, indexed),
		tile("keys advertised", count(indexed), "action hashes in /_index"),
		tile("in flight", `${count(inFlight)} / ${count(limit)}`, rejected ? `${count(rejected)} shed with 503` : "nothing shed", rejected ? "warn" : ""),
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
		["requested", "found", "prefetched", "suppressed", "streamed"].map((k) =>
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

// push feeds one sample to a <perf-graph>. The element is defined by a module
// fetched at run time, so an early poll can land before it upgrades; a plain
// element has no push and the sample is dropped rather than throwing.
function push(id, v) {
	const el = $(id);
	if (el && typeof el.push === "function") el.push(v);
}

// The graphs are gauges over time, so each one takes the value as it stands.
// The rate is the exception: the server reports a counter, and a rate is the
// difference between two samples this page took.
function drawRate(stats) {
	push("g-inflight", value(stats, "cache_http_in_flight_requests"));
	push("g-memory", value(stats, "s3_memory_in_use_bytes") / 1024 ** 2);

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
		$("g-rate").clear?.();
		return;
	}
	if (dt <= 0) return;

	const now = dv / dt;
	state.peak = Math.max(state.peak, now);
	push("g-rate", now);
	$("rate-label").textContent = `${now.toFixed(1)} req/s now, peak ${state.peak.toFixed(1)} since this page loaded`;
}

// Every entry here is a number that should be zero, or should be falling. The
// alert flag turns the value red so a rising one is visible without reading
// the labels.
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
	drawTripwires(stats);
	drawMemory(stats);
	drawConfig(stats);
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
}

// The toggle is a custom element whose module is deferred, and this script is
// a classic one at the end of the body, so it runs first. Until the element
// upgrades it carries the attribute and no property, and reading the property
// alone reports "off" and stops the page polling at all.
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

poll();
schedule();
// Once the element upgrades its property is authoritative; re-read it in case
// it disagrees with the attribute this started on.
customElements.whenDefined("scratch-toggle").then(schedule);
