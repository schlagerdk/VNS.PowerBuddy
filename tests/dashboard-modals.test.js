const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(
	path.join(__dirname, '../src/powerbuddy/static/scripts/powerbuddy.js'), 'utf8'
);
const menuMarker = source.indexOf("const menuButton = document.getElementById('pbHeaderMenuButton');");
const start = source.lastIndexOf('(function () {', menuMarker);
const end = source.indexOf('\n\t\t})();', menuMarker);
assert.ok(menuMarker >= 0 && start >= 0 && end > start, 'dashboard menu handler not found');
const script = new vm.Script(source.slice(start, end + '\n\t\t})();'.length));

class Element {
	constructor(attributes = {}) {
		this.attributes = attributes;
		this.listeners = {};
		const styles = new Map();
		this.style = {
			setProperty: (name, value) => styles.set(name, value),
			removeProperty: (name) => styles.delete(name),
			getPropertyValue: (name) => styles.get(name) || '',
		};
		const classes = new Set();
		this.classList = {
			add: (...names) => names.forEach((name) => classes.add(name)),
			remove: (...names) => names.forEach((name) => classes.delete(name)),
			contains: (name) => classes.has(name),
			toggle: (name, force = !classes.has(name)) => {
				if (force) classes.add(name);
				else classes.delete(name);
				return force;
			},
		};
	}

	addEventListener(type, listener) {
		(this.listeners[type] ||= []).push(listener);
	}

	setAttribute(name, value) {
		this.attributes[name] = String(value);
	}

	getAttribute(name) {
		return this.attributes[name] ?? null;
	}

	click(target = this) {
		for (const listener of this.listeners.click || []) listener({ target });
	}
}

function dashboard() {
	const logs = new Element();
	const settings = new Element();
	const login = new Element();
	const logClose = new Element();
	const settingsClose = new Element();
	const loginClose = new Element();
	const logMenu = new Element({ 'data-menu-action': 'logs' });
	const settingsMenu = new Element({ 'data-menu-action': 'settings' });
	const elements = {
		pbLogModal: logs,
		pbLogModalClose: logClose,
		pbSettingsModal: settings,
		pbSettingsModalClose: settingsClose,
		pbHeaderLoginModal: login,
		pbHeaderLoginClose: loginClose,
	};
	const document = {
		body: new Element(),
		getElementById: (id) => elements[id] || null,
		querySelectorAll: (selector) => selector === '[data-menu-action]' ? [logMenu, settingsMenu] : [],
		querySelector: () => [logs, settings, login].find((item) => item.classList.contains('open')) || null,
		addEventListener: () => {},
	};
	const window = {
		PB_LANG: 'da',
		PB_I18N: {},
		location: { href: 'https://test.invalid/powerbuddy', search: '' },
		scrollY: 0,
		scrollTo: (x, y) => { window.scrollY = y; },
		setTimeout: () => 0,
		clearTimeout: () => {},
		addEventListener: () => {},
		matchMedia: () => ({ matches: false }),
	};
	const fetch = async () => ({ ok: true, json: async () => ({ ok: true, authorized: true, lines: [] }) });
	script.runInNewContext({
		document, window, fetch, URL, URLSearchParams,
		pbT: (key) => key,
	});
	return { logs, settings, login, logClose, settingsClose, logMenu, settingsMenu, document, window };
}

test('log and settings stay open on backdrop clicks and close with their X', () => {
	const ui = dashboard();
	for (const [modal, menu, close] of [
		[ui.logs, ui.logMenu, ui.logClose],
		[ui.settings, ui.settingsMenu, ui.settingsClose],
	]) {
		menu.click();
		assert.equal(modal.classList.contains('open'), true);
		assert.equal(modal.getAttribute('aria-hidden'), 'false');
		assert.equal(ui.document.body.classList.contains('pb-modal-open'), true);
		modal.click();
		assert.equal(modal.classList.contains('open'), true);
		assert.equal(modal.getAttribute('aria-hidden'), 'false');
		close.click();
		assert.equal(modal.classList.contains('open'), false);
		assert.equal(modal.getAttribute('aria-hidden'), 'true');
		assert.equal(ui.document.body.classList.contains('pb-modal-open'), false);
	}
});

test('the login modal still closes when its backdrop is clicked', () => {
	const ui = dashboard();
	ui.login.classList.add('open');
	ui.login.setAttribute('aria-hidden', 'false');
	ui.login.click();
	assert.equal(ui.login.classList.contains('open'), false);
	assert.equal(ui.login.getAttribute('aria-hidden'), 'true');
});

test('settings lock and restore the page position, even if opened again', () => {
	const ui = dashboard();
	ui.window.scrollY = 240;
	ui.settingsMenu.click();
	assert.equal(ui.document.body.classList.contains('pb-settings-scroll-lock'), true);
	assert.equal(ui.document.body.style.getPropertyValue('--pb-settings-page-offset'), '-240px');

	ui.window.scrollY = 0;
	ui.settingsMenu.click();
	assert.equal(ui.document.body.style.getPropertyValue('--pb-settings-page-offset'), '-240px');
	ui.settings.click();
	assert.equal(ui.document.body.classList.contains('pb-settings-scroll-lock'), true);

	ui.settingsClose.click();
	assert.equal(ui.window.scrollY, 240);
	assert.equal(ui.document.body.classList.contains('pb-settings-scroll-lock'), false);
	assert.equal(ui.document.body.classList.contains('pb-modal-open'), false);
	assert.equal(ui.document.body.style.getPropertyValue('--pb-settings-page-offset'), '');
});

test('closing settings leaves the general lock in place while another modal is open', () => {
	const ui = dashboard();
	ui.window.scrollY = 120;
	ui.settingsMenu.click();
	ui.logs.classList.add('open');

	ui.settingsClose.click();
	assert.equal(ui.window.scrollY, 120);
	assert.equal(ui.document.body.classList.contains('pb-settings-scroll-lock'), false);
	assert.equal(ui.document.body.classList.contains('pb-modal-open'), true);

	ui.logClose.click();
	assert.equal(ui.document.body.classList.contains('pb-modal-open'), false);
});
