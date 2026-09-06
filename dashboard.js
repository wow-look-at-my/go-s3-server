// The dashboard polls one JSON snapshot and redraws. It keeps no state on the
// server: a rate is the difference between two samples this page took, so a
// reload starts the rate over and nothing else.

const POLL_MS = 5000;
const HISTORY = 60;

const state = {
	previous: null, // the last snapshot, for rates
	rates: [], // requests per second, newest last
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

function bar(name, n, max, tone, right) {
	const el = document.createElement("div");
	el.className = tone ? `bar ${tone}` : "bar";
	el.innerHTML = `<span class="name"></span><span class="n"></span><span class="track"><span class="fill"></span></span>`;
	el.querySelector(".name").textContent = name;
	el.querySelector(".n").textContent = right !== undefined ? right : count(n);
	const width = max > 0 ? Math.min(100, (n / max) * 100) : 0;
	el.querySelector(".fill").style.width = `${width}%`;
	return el;
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

function drawTiles(stats) {
	const outcomes = series(stats, "s3_get_requests_total");
	const reads = sum(outcomes);
	const hits = outcomes.hit || 0;
	const cacheBytes = value(stats, "s3_cache_bytes");
	const budget = stats.eviction.max_bytes;
	const inFlight = value(stats, "cache_http_in_flight_requests");
	const limit = stats.server.max_concurrent_requests;
	const indexed = value(stats, "s3_index_hashes") + value(stats, "s3_index_pending_hashes");
	const rejected = value(stats, "s3_http_rejected_total");

	fill($("tiles"), [
		tile("hit rate", reads ? percent(hits, reads) : "--", `${count(hits)} of ${count(reads)} single GETs`),
		tile("cache size", bytes(cacheBytes), budget ? `${percent(cacheBytes, budget)} of ${bytes(budget)}` : "no size budget", budget && cacheBytes > budget * 0.9 ? "warn" : ""),
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
	if (!Object.keys(outcomes).length) {
		fill($("get-outcomes"), [Object.assign(document.createElement("p"), { className: "muted", textContent: "no single-object GETs yet" })]);
	}

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
		entries.length ? entries.map(([k, v]) => bar(k, v, max)) : [Object.assign(document.createElement("p"), { className: "muted", textContent: "no requests yet" })],
	);
	drawRate(stats);
}

function drawRate(stats) {
	const total = sum(series(stats, "cache_http_requests_total"));
	const prev = state.previous;
	if (prev) {
		const dt = (new Date(stats.generated_at) - new Date(prev.generated_at)) / 1000;
		const dv = total - prev.total;
		// A counter that went backwards means the server restarted between
		// samples. Drop the point rather than draw a negative rate.
		if (dt > 0 && dv >= 0) {
			state.rates.push(dv / dt);
			if (state.rates.length > HISTORY) state.rates.shift();
		} else if (dv < 0) {
			state.rates = [];
		}
	}
	state.previous = { total, generated_at: stats.generated_at };

	const svg = $("rate-chart");
	if (state.rates.length < 2) return;
	const peak = Math.max(...state.rates, 0.001);
	const step = 320 / (HISTORY - 1);
	const d = state.rates
		.map((r, i) => {
			const x = (i + (HISTORY - state.rates.length)) * step;
			const y = 85 - (r / peak) * 80;
			return `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
		})
		.join(" ");
	const path = svg.querySelector("path") || svg.appendChild(document.createElementNS("http://www.w3.org/2000/svg", "path"));
	path.setAttribute("d", d);
	const now = state.rates[state.rates.length - 1];
	$("rate-label").textContent = `${now.toFixed(1)} req/s now, peak ${peak.toFixed(1)} over the last ${Math.round((state.rates.length * POLL_MS) / 1000)}s`;
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
		nodes.push(Object.assign(document.createElement("p"), { className: "muted", textContent: "no process memory limit discovered; caches use fixed default budgets" }));
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
	const pill = $("state");
	pill.textContent = stats.server.draining ? "draining" : "serving";
	pill.className = stats.server.draining ? "pill warn" : "pill ok";
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
		const pill = $("state");
		pill.textContent = "unreachable";
		pill.className = "pill bad";
		$("footer-note").textContent = `last poll failed: ${err.message}. `;
	}
}

function schedule() {
	clearInterval(state.timer);
	if ($("autorefresh").checked) state.timer = setInterval(poll, POLL_MS);
}

$("autorefresh").addEventListener("change", () => {
	schedule();
	if ($("autorefresh").checked) poll();
});

poll();
schedule();
