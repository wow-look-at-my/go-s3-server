// Browser check for the dashboard's BY PROJECT panel.
//
// The Go tests cover the counters and the snapshot. What they cannot reach is
// the page: that the per-project series becomes a stacked chart, that the
// bands paint, and that the miss table names the project that is missing.
// This starts the real server, moves real objects through it under different
// project headers, and reads the painted pixels back.
//
// go-toolchain # builds build/go-s3-server
//
// The page imports <perf-graph> from the js-snippets library site at run
// time. That site carries the MERGED library, so an unmerged branch has no
// stacked mode there yet. This check therefore takes the module from a local
// js-snippets build, and it fails when there is none rather than testing a
// component nobody changed. JS_SNIPPETS_DIST moves where it looks.

import { createRequire } from 'node:module';
const { chromium } = createRequire(import.meta.url)('playwright');
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const BINARY = process.env.GO_S3_SERVER ?? 'build/go-s3-server';
const DIST = process.env.JS_SNIPPETS_DIST ?? '/home/user/js-snippets/dist';
// The HTML names the library site through its `@library` alias. The module
// names it through the `branch/library` path. Both spellings have to land on
// the local build, or the component quietly comes from the network.
const LIBRARY = 'https://sites.pazer.build/js-snippets/';
const ALIASES = ['@library/', 'branch/library/'];
const BUCKET = 'dashboard-check';
const API = 19010;
const DASHBOARD = 19012;
/** Long enough for several polls: the chart is a rate, and a single snapshot draws nothing. */
const SETTLE_MS = Number(process.env.DASHBOARD_SETTLE_MS ?? 5000);

const failures = [];
const check = (ok, what) => {
	if (!ok) failures.push(what);
};

const work = mkdtempSync(join(tmpdir(), 'dashboard-check-'));
const configPath = join(work, 'config.json');
writeFileSync(
	configPath,
	JSON.stringify({
		listen: `127.0.0.1:${API}`,
		metrics_listen: `127.0.0.1:${API + 1}`,
		dashboard_listen: `127.0.0.1:${DASHBOARD}`,
		bucket: BUCKET,
		data_dir: join(work, 'data'),
		disable_auth: true,
	}),
);

const server = spawn(resolve(BINARY), ['--config', configPath], { stdio: ['ignore', 'pipe', 'pipe'] });
const serverLog = [];
server.stdout.on('data', (b) => serverLog.push(String(b)));
server.stderr.on('data', (b) => serverLog.push(String(b)));

const die = async (message, browser) => {
	if (browser) await browser.close();
	server.kill();
	console.error(`check-dashboard FAILED: ${message}\n${serverLog.join('')}`);
	process.exit(1);
};

// Wait for the API to answer rather than sleeping a guessed amount.
const until = async (fn, what) => {
	const deadline = Date.now() + 20000;
	for (;;) {
		try {
			if (await fn()) return;
		} catch {
		}
		if (Date.now() > deadline) await die(`timed out waiting for ${what}`);
		await new Promise((r) => setTimeout(r, 200));
	}
};

await until(async () => (await fetch(`http://127.0.0.1:${API}/_health`)).ok, 'the cache API');

// Traffic under project names: a single that hits, a single that only misses,
// and a single that only stores. The panel has to tell them apart.
const url = (n) => `http://127.0.0.1:${API}/${BUCKET}/${'0'.repeat(56)}${String(n).padStart(8, '0')}`;
const put = (project, n) =>
	fetch(url(n), {
		method: 'PUT',
		headers: { 'X-Cache-Module': project, 'X-Cache-Meta-outputid': 'x'.repeat(64) },
		body: `body-${n}`,
	});
const get = (project, n) => fetch(url(n), { headers: { 'X-Cache-Module': project } });

for (let n = 0; n < 12; n++) await put('github.com/wow-look-at-my/go-toolchain', n);
for (let n = 100; n < 106; n++) await put('github.com/wow-look-at-my/js-snippets', n);

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1400, height: 1000 }, deviceScaleFactor: 2 });
// An uncaught exception fails the run. A console error only gets reported:
// the page chrome logs a single when the Google webfonts its tokens name
// are blocked, which says nothing about whether the panel drew.
const errors = [];
const notes = [];
page.on('console', (m) => {
	if (m.type() === 'error') notes.push(m.text());
});
page.on('pageerror', (e) => errors.push(String(e)));

// Serve the library modules from a local build when a single is there, so a
// branch is verifiable before its library publish lands.
const local = existsSync(join(DIST, 'ui/perf-graph.js'));
check(local, `no local js-snippets build at ${DIST}: run pnpm build there first`);
console.log(`serving <perf-graph> from ${DIST}`);
if (local) {
	await page.route(`${LIBRARY}**`, async (route) => {
		let rest = route.request().url().slice(LIBRARY.length);
		for (const alias of ALIASES) if (rest.startsWith(alias)) rest = rest.slice(alias.length);
		const path = join(DIST, rest);
		if (!existsSync(path)) return route.fulfill({ status: 404, body: '' });
		await route.fulfill({ contentType: 'text/javascript', body: readFileSync(path, 'utf8') });
	});
}
// The page chrome loads for real. It cannot be stubbed: the auto-refresh
// switch is a scratch-ui element, and the poll loop reads `checked` off it.
// A stub leaves that undefined. The page then polls a single time and
// stops, and every rate on it sits on its waiting-for-a-sample text forever.
//
// Node fetches it and hands the bytes to the page, rather than the browser
// fetching it: this sandbox reaches the internet through a proxy whose CA the
// bundled chromium does not carry, and turning the browser's certificate
// checking off to work around that is not a trade this check makes.
await page.route('https://sites.pazer.build/scratch_ui/**', async (route) => {
	const url = route.request().url();
	const res = await fetch(url);
	if (!res.ok) return route.fulfill({ status: res.status, body: '' });
	await route.fulfill({
		contentType: url.endsWith('.css') ? 'text/css' : 'text/javascript',
		body: await res.text(),
	});
});

await page.goto(`http://127.0.0.1:${DASHBOARD}/`, { waitUntil: 'load' });

// Keep asking for keys that are not there while the page polls, so the miss
// band and the miss table both have something to report.
const missing = setInterval(() => {
	for (let n = 900; n < 910; n++) void get('github.com/wow-look-at-my/gosmopolitan', n);
	for (let n = 0; n < 6; n++) void get('github.com/wow-look-at-my/go-toolchain', n);
}, 400);
await page.waitForTimeout(SETTLE_MS);
clearInterval(missing);
await page.waitForTimeout(1500);

const report = await page.evaluate(() => {
	const chart = document.getElementById('project-chart');
	const canvas = chart?.shadowRoot?.querySelector('canvas');
	const out = {
		stacked: chart?.stacked ?? null,
		keys: chart?.series?.map((s) => s.key) ?? [],
		label: document.getElementById('project-label')?.textContent ?? '',
		missRows: [...document.querySelectorAll('#project-misses tbody tr')].map((tr) =>
			[...tr.children].map((td) => td.textContent),
		),
		colors: 0,
	};
	if (canvas) {
		// The newest column, not the middle: the page starts with no history, so
		// after a few polls the bands hug the right edge and the middle is still
		// background.
		const data = canvas.getContext('2d').getImageData(canvas.width - 4, 26, 1, canvas.height - 56).data;
		const seen = new Set();
		for (let i = 0; i < data.length; i += 4) if (data[i + 3] !== 0) seen.add(`${data[i]},${data[i + 1]},${data[i + 2]}`);
		out.colors = seen.size;
	}
	return out;
});

check(report.stacked === true, `the chart is in stacked mode (got ${report.stacked})`);
check(report.keys.length >= 3, `every project that moved an object is a band (got ${report.keys.join(' ')})`);
check(report.keys.join(',') === [...report.keys].sort().join(','), 'the bands are ordered by name');
// Background, gridline, and a single color per band with height.
check(report.colors >= 3, `the bands paint (${report.colors} distinct colors in one column)`);
check(/objects\/s across/.test(report.label), `the label reports the total (got "${report.label}")`);
const gosmo = report.missRows.find((r) => r[0].includes('gosmopolitan'));
check(gosmo !== undefined, 'the project that only missed is in the miss table');
check(gosmo === undefined || gosmo[1].startsWith('100.0%'), `it reads as missing everything (got ${gosmo?.[1]})`);

const panel = await page.$('.projects');
if (panel) await panel.screenshot({ path: process.argv[2] ?? join(work, 'by-project.png') });
else failures.push('no BY PROJECT panel on the page');
console.log(`screenshot: ${process.argv[2] ?? join(work, 'by-project.png')}`);

await browser.close();
server.kill();

if (notes.length > 0) console.log('console errors (not failures):\n  ' + notes.slice(0, 5).join('\n  '));
if (errors.length > 0) failures.push('uncaught page errors:\n    ' + errors.slice(0, 5).join('\n    '));
if (failures.length > 0) {
	console.error('check-dashboard FAILED:\n  ' + failures.join('\n  '));
	console.error(JSON.stringify(report, null, 2));
	process.exit(1);
}
console.log('check-dashboard OK');
console.log(JSON.stringify(report, null, 2));
