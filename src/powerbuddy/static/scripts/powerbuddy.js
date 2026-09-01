		(function () {
			const ua = navigator.userAgent || '';
			if (ua.indexOf('Electron') >= 0) {
				document.body.classList.add('electron-performance');
			}
		})();

		(function () {
			const chart = document.getElementById("priceChart");
			if (!chart) return;

			const rangeToggle = chart.querySelector(".range-toggle");
			const chartScroll = chart.querySelector(".chart-scroll");
			const chartBars = chart.querySelector(".chart-bars");
			const buttons = Array.from(chart.querySelectorAll(".range-btn"));
			const bars = Array.from(chart.querySelectorAll(".bar-wrapper"));
			const chartDataArea = document.getElementById("pbChartDataArea");
			const isOverviewMode = chart.getAttribute("data-overview-mode") === "1";
			const availableBars = bars.length;
			const rangeStorageKey = isOverviewMode ? 'pb-chart-range-hours-overview' : 'pb-chart-range-hours';
			const scrollStorageKey = isOverviewMode ? 'pb-chart-scroll-overview' : 'pb-chart-scroll';
			const scrollHourMarkerKey = isOverviewMode ? 'pb-chart-scroll-hour-overview' : 'pb-chart-scroll-hour';
			const shortRange = 12;
			const longRange = availableBars;
			const forceLongRange = !isOverviewMode;
			if (isOverviewMode) {
				chart.classList.add("overview-mode");
			}

			function loadSavedRange() {
				try {
					const raw = window.localStorage.getItem(rangeStorageKey);
					if (!raw) return null;
					const parsed = Number(raw);
					return Number.isFinite(parsed) ? parsed : null;
				} catch (_) {
					return null;
				}
			}

			function saveRange(range) {
				try {
					window.localStorage.setItem(rangeStorageKey, String(range));
				} catch (_) {
					// Ignore storage errors (private mode, quota, etc.)
				}
			}

			function loadSavedScrollLeft() {
				try {
					const raw = window.sessionStorage.getItem(scrollStorageKey);
					if (!raw) return null;
					const parsed = Number(raw);
					return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
				} catch (_) {
					return null;
				}
			}

			function saveScrollLeft(scrollLeft) {
				try {
					window.sessionStorage.setItem(scrollStorageKey, String(Math.max(0, scrollLeft || 0)));
					window.sessionStorage.setItem(scrollHourMarkerKey, getCurrentHourMarker());
				} catch (_) {
					// Ignore storage errors (private mode, quota, etc.)
				}
			}

			function loadSavedHourMarker() {
				try {
					return window.sessionStorage.getItem(scrollHourMarkerKey) || null;
				} catch (_) {
					return null;
				}
			}

			function getCurrentHourMarker() {
				const serverMarker = (chart.getAttribute("data-current-start-time") || "").trim();
				if (serverMarker) {
					return serverMarker;
				}

				const now = new Date();
				const yyyy = String(now.getFullYear());
				const mm = String(now.getMonth() + 1).padStart(2, "0");
				const dd = String(now.getDate()).padStart(2, "0");
				const hh = String(now.getHours()).padStart(2, "0");
				return `${yyyy}-${mm}-${dd}T${hh}.00.00`;
			}

			function scrollToCurrentHourBar() {
				if (!chartScroll || !bars.length) return false;

				const currentMarker = getCurrentHourMarker();
				const currentBar = bars.find((bar) => (bar.getAttribute("data-start-time") || "") === currentMarker);
				if (!currentBar) return false;

				const targetLeft = Math.max(0, currentBar.offsetLeft - 16);
				const applyTargetScroll = () => {
					chartScroll.scrollLeft = targetLeft;
				};

				window.requestAnimationFrame(applyTargetScroll);
				window.setTimeout(applyTargetScroll, 80);
				window.setTimeout(applyTargetScroll, 220);
				saveScrollLeft(targetLeft);
				return true;
			}

			function restoreChartScrollLeft() {
				if (!chartScroll) return;
				const shouldPreferSavedScroll = new URLSearchParams(window.location.search).has("pbRefresh");
				if (!shouldPreferSavedScroll) {
					scrollToCurrentHourBar();
					return;
				}

				const currentHourMarker = getCurrentHourMarker();
				const savedHourMarker = loadSavedHourMarker();
				if (savedHourMarker !== currentHourMarker) {
					scrollToCurrentHourBar();
					return;
				}

				const savedScrollLeft = loadSavedScrollLeft();
				if (savedScrollLeft == null) {
					scrollToCurrentHourBar();
					return;
				}

				const applySavedScroll = () => {
					chartScroll.scrollLeft = savedScrollLeft;
				};

				window.requestAnimationFrame(applySavedScroll);
				window.setTimeout(applySavedScroll, 80);
				window.setTimeout(applySavedScroll, 220);
			}

			if (buttons[0]) {
				const shortLabel = availableBars <= shortRange ? `${availableBars}t` : `${shortRange}t`;
				buttons[0].textContent = shortLabel;
				buttons[0].setAttribute("data-range", String(Math.min(shortRange, availableBars)));
			}

			if (buttons[1]) {
				buttons[1].textContent = 'I dag+I morgen';
				buttons[1].setAttribute("data-range", String(longRange));
			}

			const mobileViewportQuery = window.matchMedia("(max-width: 900px)");
			const compactDesktopViewportQuery = window.matchMedia("(min-width: 901px) and (max-width: 1366px) and (orientation: landscape)");

			function isMobileForcedLongRange() {
				return forceLongRange;
			}

			function refreshRangeToggleVisibility() {
				if (!rangeToggle) return;
				const shouldHide = true;
				rangeToggle.style.display = shouldHide ? "none" : "inline-flex";
			}

			function fitChartHeightToViewport() {
				if (!chartBars || !chartDataArea) return;

				const isMobileViewport = mobileViewportQuery.matches;
				const isCompactDesktopViewport = compactDesktopViewportQuery.matches;

				const viewportHeight = window.visualViewport ? window.visualViewport.height : window.innerHeight;
				const viewportBottom = viewportHeight;
				const chartRect = chart.getBoundingClientRect();
				const barsRect = chartBars.getBoundingClientRect();
				const chartStyles = window.getComputedStyle(chart);
				const chartMarginBottom = Number.parseFloat(chartStyles.marginBottom || '0') || 0;
				const pageEl = chart.closest('.page');
				const pagePaddingBottom = pageEl ? (Number.parseFloat(window.getComputedStyle(pageEl).paddingBottom || '0') || 0) : 0;
				const structuralBottomSpace = (isMobileViewport || isOverviewMode) ? 0 : (chartMarginBottom + pagePaddingBottom);
				const bottomGap = (isOverviewMode ? 20 : (isMobileViewport ? 42 : 20)) + structuralBottomSpace;
				const chartOverhead = Math.max(0, chartRect.height - barsRect.height);
				const availableHeight = viewportBottom - chartRect.top - bottomGap - chartOverhead;
				const minHeight = isOverviewMode ? 140 : (isMobileViewport ? 96 : (isCompactDesktopViewport ? 264 : 292));
				const maxHeight = isOverviewMode ? 492 : (isMobileViewport ? 320 : (isCompactDesktopViewport ? 464 : 492));
				const clampedHeight = Math.min(maxHeight, Math.max(minHeight, Math.floor(availableHeight)));

				chart.style.setProperty("--pb-chart-height", `${clampedHeight}px`);
				chart.style.setProperty("--pb-mobile-chart-height", `${clampedHeight}px`);
				if (isOverviewMode) {
					chartBars.style.height = `${clampedHeight}px`;
				}

				// Correct tiny residual overflow (often 1-10px) caused by mobile viewport rounding.
				const fittedRect = chart.getBoundingClientRect();
				const overflowPx = Math.ceil(fittedRect.bottom - (viewportBottom - 8));
				if (overflowPx > 0) {
					const adjustedHeight = Math.max(minHeight, clampedHeight - overflowPx - 2);
					chart.style.setProperty("--pb-chart-height", `${adjustedHeight}px`);
					chart.style.setProperty("--pb-mobile-chart-height", `${adjustedHeight}px`);
					if (isOverviewMode) {
						chartBars.style.height = `${adjustedHeight}px`;
					}
				}
			}

			let mobileFitFrame = 0;
			function scheduleViewportFit() {
				if (mobileFitFrame) {
					window.cancelAnimationFrame(mobileFitFrame);
				}

				mobileFitFrame = window.requestAnimationFrame(() => {
					mobileFitFrame = 0;
					fitChartHeightToViewport();
				});
			}

			function updateCounts(range) {
				chart.classList.add("compact-top");
			}

			function applyRange(range, persistSelection) {
				const normalizedRange = (isOverviewMode || isMobileForcedLongRange())
					? longRange
					: Math.max(1, Math.min(range, availableBars));
				chart.classList.toggle("show-12", !isOverviewMode && normalizedRange <= shortRange);
				chart.classList.toggle("show-24", isOverviewMode || normalizedRange > shortRange);

				const visibleCount = normalizedRange;
				if (chartBars) {
					chartBars.style.setProperty("--bar-cols", String(visibleCount));
				}

				bars.forEach((bar, index) => {
					bar.classList.toggle("is-hidden", index >= visibleCount);
					bar.classList.remove("edge-left", "edge-right");
					bar.querySelectorAll(".bar-flag").forEach((flag) => {
						flag.classList.remove("first", "last");
					});
				});

				const shouldApplyEdgeClasses = !mobileViewportQuery.matches && visibleCount > 17;
				if (shouldApplyEdgeClasses) {
					const visibleBars = bars.filter((_, index) => index < visibleCount);
					visibleBars.forEach((bar, index) => {
						if (index === 0) {
							bar.classList.add("edge-left");
							bar.querySelectorAll(".bar-flag").forEach((flag) => {
								flag.classList.add("first");
							});
						}
						if (index === visibleBars.length - 1) {
							bar.classList.add("edge-right");
							bar.querySelectorAll(".bar-flag").forEach((flag) => {
								flag.classList.add("last");
							});
						}
					});
				}

				buttons.forEach((btn) => {
					const value = Number(btn.getAttribute("data-range"));
					btn.classList.toggle("active", value === normalizedRange);
				});

				updateCounts(normalizedRange);
				scheduleViewportFit();
				if (persistSelection && !isOverviewMode && !isMobileForcedLongRange()) {
					saveRange(normalizedRange);
				}
			}

			function syncRangeModeWithViewport() {
				refreshRangeToggleVisibility();
				if (isOverviewMode) {
					applyRange(longRange, false);
					return;
				}

				if (isMobileForcedLongRange()) {
					applyRange(longRange, false);
					return;
				}

				const fallbackRange = availableBars <= shortRange ? availableBars : shortRange;
				const saved = loadSavedRange();
				const desktopRange = saved == null ? fallbackRange : Math.max(1, Math.min(saved, availableBars));
				applyRange(desktopRange, false);
			}

			if (!isOverviewMode && !forceLongRange) {
				buttons.forEach((btn) => {
					btn.addEventListener("click", () => {
						const range = Number(btn.getAttribute("data-range"));
						applyRange(range, true);
					});
				});
			}

			refreshRangeToggleVisibility();

			const defaultRange = (isOverviewMode || forceLongRange)
				? longRange
				: (availableBars <= shortRange ? availableBars : shortRange);
			const savedRange = isOverviewMode ? null : loadSavedRange();
			const initialRange = savedRange == null ? defaultRange : Math.max(1, Math.min(savedRange, availableBars));
			applyRange(initialRange, false);
			restoreChartScrollLeft();

			if (chartScroll) {
				let scrollSaveFrame = 0;
				chartScroll.addEventListener('scroll', () => {
					if (scrollSaveFrame) {
						window.cancelAnimationFrame(scrollSaveFrame);
					}

					scrollSaveFrame = window.requestAnimationFrame(() => {
						scrollSaveFrame = 0;
						saveScrollLeft(chartScroll.scrollLeft);
					});
				}, { passive: true });

				window.addEventListener('beforeunload', () => {
					saveScrollLeft(chartScroll.scrollLeft);
				});
			}

			window.addEventListener("resize", scheduleViewportFit, { passive: true });
			window.addEventListener("orientationchange", scheduleViewportFit, { passive: true });
			if (window.visualViewport) {
				window.visualViewport.addEventListener("resize", scheduleViewportFit, { passive: true });
			}
			if (typeof mobileViewportQuery.addEventListener === "function") {
				mobileViewportQuery.addEventListener("change", syncRangeModeWithViewport);
			} else if (typeof mobileViewportQuery.addListener === "function") {
				mobileViewportQuery.addListener(syncRangeModeWithViewport);
			}
			window.addEventListener("message", (event) => {
				const data = event && event.data ? event.data : null;
				const type = typeof data === "string" ? data : data && data.type;
				if (type !== "powerbuddy-modal-opened") return;

				scheduleViewportFit();
				window.setTimeout(scheduleViewportFit, 80);
				window.setTimeout(scheduleViewportFit, 180);
			});
			scheduleViewportFit();
		})();

		(function () {
			const modal = document.getElementById('pbActionModal');
			if (!modal) return;

			const closeBtn = document.getElementById('pbActionModalClose');
			const slot = document.getElementById('pbActionSlot');
			const currentActionChip = document.getElementById('pbCurrentActionChip');
			const status = document.getElementById('pbActionModalStatus');
			const loginPanel = document.getElementById('pbActionLoginPanel');
			const editorPanel = document.getElementById('pbActionEditorPanel');
			const loginForm = document.getElementById('pbActionLoginForm');
			const loginPassword = document.getElementById('pbActionLoginPassword');
			const loginSubmit = document.getElementById('pbActionLoginSubmit');
			const choiceButtons = Array.from(modal.querySelectorAll('.pb-action-choice'));
			const actionRiskConfirm = document.getElementById('pbActionRiskConfirm');
			const actionRiskTitle = document.getElementById('pbActionRiskTitle');
			const actionRiskText = document.getElementById('pbActionRiskText');
			const actionRiskCancel = document.getElementById('pbActionRiskCancel');
			const actionRiskContinue = document.getElementById('pbActionRiskContinue');
			const actionChoices = modal.querySelector('.pb-action-choices');
			const actionTargets = Array.from(document.querySelectorAll('.bar-wrapper.is-action-editable'));
			const chart = document.getElementById('priceChart');
			const chartRunToggle = document.getElementById('pbChartRunToggle');
			const chartStartBtn = document.getElementById('pbChartStartBtn');
			const chartPausedPanel = document.getElementById('pbChartPausedPanel');
			const controlConfirmModal = document.getElementById('pbControlConfirmModal');
			const controlConfirmClose = document.getElementById('pbControlConfirmClose');
			const controlConfirmTitle = document.getElementById('pbControlConfirmTitle');
			const controlConfirmText = document.getElementById('pbControlConfirmText');
			const controlConfirmCategory = document.getElementById('pbControlConfirmCategory');
			const controlConfirmAction = document.getElementById('pbControlConfirmAction');
			const controlConfirmPrice = document.getElementById('pbControlConfirmPrice');
			const controlConfirmCancel = document.getElementById('pbControlConfirmCancel');
			const controlConfirmContinue = document.getElementById('pbControlConfirmContinue');

			let activeAction = 'auto';
			let currentAction = 'auto';
			let activeActionId = '';
			let activeStartTime = '';
			let activeCategory = '';
			let activePrice = null;
			let isSaving = false;
			let isControlBusy = false;
			let actionRiskConfirmPending = null;
			let controlConfirmPending = null;
			let modalMode = 'action';
			let pendingControlCommand = '';
			let isPowerBuddyPaused = chart ? chart.getAttribute('data-is-paused') === '1' : false;
			let currentControlAction = chart ? (chart.getAttribute('data-current-action') || '').toLowerCase() : '';
			let currentControlCategory = chart ? (chart.getAttribute('data-current-category') || '').toLowerCase() : '';
			let currentControlPrice = chart ? Number.parseFloat(chart.getAttribute('data-current-price') || '') : Number.NaN;
			const expensiveChargeThresholdKr = 2.5;

			const cheapDischargeThresholdKr = 1.5;

			function persistChartScrollForReload() {
				if (!chart) {
					return;
				}

				const scrollEl = chart.querySelector('.chart-scroll');
				if (!scrollEl) {
					return;
				}

				const isOverview = chart.getAttribute('data-overview-mode') === '1';
				const storageKey = isOverview ? 'pb-chart-scroll-overview' : 'pb-chart-scroll';
				try {
					window.sessionStorage.setItem(storageKey, String(Math.max(0, scrollEl.scrollLeft || 0)));
				} catch (_) {
					// Ignore storage errors.
				}
			}

			function actionIconSvg(actionName) {
				switch ((actionName || '').toLowerCase()) {
					case 'charge':
						return '<svg viewBox="0 0 16 16"><path d="M8 13V3"></path><path d="M4.5 6.5L8 3l3.5 3.5"></path></svg>';
					case 'discharge':
						return '<svg viewBox="0 0 16 16"><path d="M8 3v10"></path><path d="M4.5 9.5L8 13l3.5-3.5"></path></svg>';
					case 'hold':
						return '<svg viewBox="0 0 16 16"><path d="M6 3v10"></path><path d="M10 3v10"></path></svg>';
					case 'auto':
					default:
						return '<svg viewBox="0 0 16 16"><path d="M12.8 5.2A5 5 0 0 0 4.2 3.8"></path><path d="M4.1 1.8V4h2.2"></path><path d="M3.2 10.8A5 5 0 0 0 11.8 12.2"></path><path d="M11.9 14.2V12h-2.2"></path></svg>';
				}
			}

			const heroRibbon = document.querySelector('.hero-action-ribbon');

			if (isPowerBuddyPaused && heroRibbon) {
				heroRibbon.classList.remove('charge', 'discharge', 'hold', 'auto');
				heroRibbon.classList.add('auto');
				const span = heroRibbon.querySelector('span');
				if (span) span.textContent = 'Auto';
			}

			function setHeroRibbon(actionName, label) {
				if (!heroRibbon) return;
				heroRibbon.classList.remove('charge', 'discharge', 'hold', 'auto');
				const normalized = (actionName || '').toLowerCase();
				if (normalized) heroRibbon.classList.add(normalized);
				const span = heroRibbon.querySelector('span');
				if (span) span.textContent = label || '';
			}

			function setPowerBuddyPausedUi(paused) {
				isPowerBuddyPaused = !!paused;
				if (!chart) return;

				chart.classList.toggle('is-paused', isPowerBuddyPaused);
				chart.setAttribute('data-is-paused', isPowerBuddyPaused ? '1' : '0');

				if (isPowerBuddyPaused) {
					setHeroRibbon('auto', 'Auto');
				} else if (heroRibbon) {
					const origAction = heroRibbon.getAttribute('data-action') || '';
					const origLabel = heroRibbon.getAttribute('data-action-label') || origAction;
					setHeroRibbon(origAction, origLabel);
				}

				if (chartPausedPanel) {
					chartPausedPanel.style.display = isPowerBuddyPaused ? 'flex' : 'none';
				}

				if (chartRunToggle) {
					if (isPowerBuddyPaused) {
						chartRunToggle.style.visibility = 'hidden';
						chartRunToggle.style.pointerEvents = 'none';
						chartRunToggle.setAttribute('data-command', 'start');
						chartRunToggle.setAttribute('title', 'Start PowerBuddy');
						chartRunToggle.setAttribute('aria-label', 'Start PowerBuddy');
						chartRunToggle.innerHTML = '<span class="pb-icon" aria-hidden="true"><svg viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg"><path d="M5 3.5L12 8L5 12.5V3.5Z"></path></svg></span><span>Start</span>';
					} else {
						chartRunToggle.style.visibility = '';
						chartRunToggle.style.pointerEvents = '';
						chartRunToggle.setAttribute('data-command', 'pause');
						chartRunToggle.setAttribute('title', 'Pause PowerBuddy');
						chartRunToggle.setAttribute('aria-label', 'Pause PowerBuddy');
						chartRunToggle.innerHTML = '<span class="pb-icon" aria-hidden="true"><svg viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg"><rect x="4" y="3" width="3" height="10" rx="1"></rect><rect x="9" y="3" width="3" height="10" rx="1"></rect></svg></span><span>Pause</span>';
					}
				}
			}

			function updateBodyScrollLock() {
				const actionOpen = modal && modal.classList.contains('open');
				const controlOpen = controlConfirmModal && controlConfirmModal.classList.contains('open');
				const isLocked = actionOpen || controlOpen;
				document.body.style.overflow = isLocked ? 'hidden' : '';
				document.body.classList.toggle('pb-modal-open', isLocked);
			}

			function setControlButtonsDisabled(disabled) {
				if (chartRunToggle) {
					chartRunToggle.disabled = disabled;
				}
				if (chartStartBtn) {
					chartStartBtn.disabled = disabled;
				}
			}

			function currentControlActionLabel() {
				switch (currentControlAction) {
					case 'charge':
						return 'Charge';
					case 'discharge':
						return 'Discharge';
					case 'hold':
						return 'Hold';
					default:
						return 'Auto';
				}
			}

			function currentControlCategoryLabel() {
				switch (currentControlCategory) {
					case 'low':
						return 'lav';
					case 'medium':
						return 'fornuftig';
					case 'high':
						return 'høj';
					case 'peak':
						return 'meget høj';
					default:
						return '';
				}
			}

			function currentControlCategoryBadgeLabel() {
				const label = currentControlCategoryLabel();
				if (!label) {
					return 'Aktuel time';
				}

				return `Pris: ${label.charAt(0).toUpperCase()}${label.slice(1)}`;
			}

			function closeControlConfirmModal() {
				if (!controlConfirmModal) {
					controlConfirmPending = null;
					return;
				}

				controlConfirmModal.classList.remove('open');
				controlConfirmModal.setAttribute('aria-hidden', 'true');
				controlConfirmPending = null;
				updateBodyScrollLock();
			}

			function pauseConfirmContent() {
				const categoryLabel = currentControlCategoryLabel();
				const priceLabel = Number.isFinite(currentControlPrice) ? formatPriceKr(currentControlPrice) : '';

				if ((currentControlCategory === 'low' || currentControlCategory === 'medium') && currentControlAction === 'charge') {
					return {
						title: 'Bekræft pause under opladning',
						text: `PowerBuddy lader lige nu i en ${categoryLabel} time${priceLabel ? ` til ${priceLabel}` : ''}. Pause stopper opladningen i den aktuelle time.`
					};
				}

				if ((currentControlCategory === 'low' || currentControlCategory === 'medium') && currentControlAction === 'hold') {
					return {
						title: 'Bekræft pause i gunstig time',
						text: `PowerBuddy holder lige nu batteriet i ro i en ${categoryLabel} time${priceLabel ? ` til ${priceLabel}` : ''}. Pause slår den aktive styring fra i den aktuelle time.`
					};
				}

				if (currentControlCategory === 'high' && currentControlAction === 'hold') {
					return {
						title: 'Bekræft pause ved høj pris',
						text: `PowerBuddy beskytter lige nu batteriet i en høj time${priceLabel ? ` til ${priceLabel}` : ''}. Pause fjerner den aktive styring i den aktuelle time.`
					};
				}

				if (currentControlCategory === 'high' && currentControlAction === 'discharge') {
					return {
						title: 'Bekræft pause under afladning',
						text: `PowerBuddy aflader lige nu i en høj time${priceLabel ? ` til ${priceLabel}` : ''}. Pause stopper afladningen i den aktuelle time.`
					};
				}

				if (currentControlCategory === 'peak' && currentControlAction === 'discharge') {
					return {
						title: 'Bekræft pause i meget dyr time',
						text: `PowerBuddy aflader lige nu i en meget høj time${priceLabel ? ` til ${priceLabel}` : ''}. Pause stopper afladningen i den aktuelle time.`
					};
				}

				return {
					title: 'Bekræft pause',
					text: `Er du sikker på, at du vil pause PowerBuddy nu${priceLabel ? ` ved ${priceLabel}` : ''}?`
				};
			}

			async function openControlConfirmDialog() {
				if (!controlConfirmModal || !controlConfirmClose || !controlConfirmTitle || !controlConfirmText || !controlConfirmCategory || !controlConfirmAction || !controlConfirmPrice || !controlConfirmCancel || !controlConfirmContinue) {
					const content = pauseConfirmContent();
					return window.confirm(content.text);
				}

				if (controlConfirmPending) {
					return controlConfirmPending;
				}

				const content = pauseConfirmContent();
				const priceLabel = Number.isFinite(currentControlPrice) ? formatPriceKr(currentControlPrice) : '';

				controlConfirmTitle.textContent = content.title;
				controlConfirmText.textContent = content.text;
				controlConfirmCategory.textContent = currentControlCategoryBadgeLabel();
				controlConfirmCategory.classList.remove('low', 'medium', 'high', 'peak');
				if (currentControlCategory === 'low' || currentControlCategory === 'medium' || currentControlCategory === 'high' || currentControlCategory === 'peak') {
					controlConfirmCategory.classList.add(currentControlCategory);
				}
				controlConfirmAction.className = `pb-current-action-chip ${(currentControlAction || 'auto')}`;
				controlConfirmAction.innerHTML = `<span class="chip-icon" aria-hidden="true">${actionIconSvg(currentControlAction || 'auto')}</span><span class="chip-label">${currentControlActionLabel()}</span>`;

				if (priceLabel) {
					controlConfirmPrice.hidden = false;
					controlConfirmPrice.textContent = priceLabel;
				} else {
					controlConfirmPrice.hidden = true;
					controlConfirmPrice.textContent = '';
				}

				controlConfirmContinue.textContent = 'Ja, pause PowerBuddy';
				controlConfirmModal.classList.add('open');
				controlConfirmModal.setAttribute('aria-hidden', 'false');
				updateBodyScrollLock();

				controlConfirmPending = new Promise((resolve) => {
					const cleanup = (result) => {
						controlConfirmClose.removeEventListener('click', onCancel);
						controlConfirmCancel.removeEventListener('click', onCancel);
						controlConfirmContinue.removeEventListener('click', onContinue);
						controlConfirmModal.removeEventListener('click', onBackdrop);
						document.removeEventListener('keydown', onKeydown);
						closeControlConfirmModal();
						resolve(result);
					};

					const onCancel = () => cleanup(false);
					const onContinue = () => cleanup(true);
					const onBackdrop = (event) => {
						if (event.target === controlConfirmModal) {
							cleanup(false);
						}
					};
					const onKeydown = (event) => {
						if (event.key === 'Escape') {
							event.preventDefault();
							cleanup(false);
						}
					};

					controlConfirmClose.addEventListener('click', onCancel);
					controlConfirmCancel.addEventListener('click', onCancel);
					controlConfirmContinue.addEventListener('click', onContinue);
					controlConfirmModal.addEventListener('click', onBackdrop);
					document.addEventListener('keydown', onKeydown);
					window.setTimeout(() => controlConfirmContinue.focus(), 0);
				});

				return controlConfirmPending;
			}

			function shouldConfirmPauseControl() {
				if (currentControlCategory === 'low' || currentControlCategory === 'medium') {
					return currentControlAction === 'hold' || currentControlAction === 'charge';
				}

				if (currentControlCategory === 'high') {
					return currentControlAction === 'hold' || currentControlAction === 'discharge';
				}

				if (currentControlCategory === 'peak') {
					return currentControlAction === 'discharge';
				}

				return false;
			}

			async function confirmPausePowerBuddyAction() {
				if (!shouldConfirmPauseControl()) {
					return true;
				}

				return openControlConfirmDialog();
			}

			function openControlLoginModal(command) {
				modalMode = 'control-login';
				pendingControlCommand = (command || '').toLowerCase();
				const loginModalAction = pendingControlCommand === 'pause'
					? (currentControlAction || 'auto')
					: (isPowerBuddyPaused ? 'hold' : 'auto');
				hideActionRiskConfirm();
				if (slot) {
					slot.textContent = 'PowerBuddy styring';
				}
				setSlotCategory('');
				setCurrentActionChip(loginModalAction);
				if (editorPanel) {
					editorPanel.style.display = 'none';
				}
				if (loginPanel) {
					loginPanel.style.display = '';
				}
				modal.classList.add('open');
				modal.setAttribute('aria-hidden', 'false');
				updateBodyScrollLock();
				setStatus(command === 'pause' ? 'Log ind for at pause PowerBuddy.' : 'Log ind for at starte PowerBuddy.', false);
				if (loginPassword) {
					window.setTimeout(() => {
						loginPassword.focus();
						loginPassword.select();
					}, 0);
				}
			}

			async function executePowerBuddyControl(command) {
				const normalized = (command || '').toLowerCase();
				if (normalized !== 'pause' && normalized !== 'start') {
					return;
				}

				if (isControlBusy) {
					return;
				}

				isControlBusy = true;
				setControlButtonsDisabled(true);
				try {
					const body = new URLSearchParams();
					body.set('command', normalized);
					const res = await fetch('?action=planning-control', {
						method: 'POST',
						headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
						body: body.toString()
					});
					const data = await res.json();

					if (res.ok && data && data.ok) {
						setPowerBuddyPausedUi(!!data.isPaused);
						setStatus('', false);
						return;
					}

					if (data && data.error === 'unauthorized') {
						openControlLoginModal(normalized);
						return;
					}

					const attempted = data && Array.isArray(data.attempted) && data.attempted.length
						? ` (${data.attempted.slice(0, 2).join(' · ')})`
						: '';
					const errCode = data && (data.error || data.message || data.statusCode)
						? String(data.error || data.message || data.statusCode)
						: 'control_failed';
					setStatus(`Kunne ikke skifte PowerBuddy status (${errCode})${attempted}.`, true);
				} catch (_) {
					setStatus('Kunne ikke skifte PowerBuddy status.', true);
				} finally {
					isControlBusy = false;
					setControlButtonsDisabled(false);
				}
			}

			async function requestControl(command) {
				const normalized = (command || '').toLowerCase();

				let authorized = false;
				try {
					authorized = await requestAuthStatus();
				} catch (_) {
					authorized = false;
				}

				if (!authorized) {
					openControlLoginModal(normalized);
					return;
				}

				if (normalized === 'pause') {
					const confirmed = await confirmPausePowerBuddyAction();
					if (!confirmed) {
						return;
					}
				}

				await executePowerBuddyControl(normalized);
			}

			function hideActionRiskConfirm() {
				if (actionRiskConfirm) {
					actionRiskConfirm.hidden = true;
				}
				actionRiskConfirmPending = null;
			}

			function isCurrentHourSelection() {
				if (!activeStartTime) {
					return false;
				}

				const slotTime = new Date(activeStartTime);
				if (Number.isNaN(slotTime.getTime())) {
					return false;
				}

				const now = new Date();
				return slotTime.getFullYear() === now.getFullYear()
					&& slotTime.getMonth() === now.getMonth()
					&& slotTime.getDate() === now.getDate()
					&& slotTime.getHours() === now.getHours();
			}

			function isExpensiveSelection() {
				if (typeof activePrice !== 'number' || Number.isNaN(activePrice)) {
					return false;
				}

				if (activeCategory === 'peak') {
					return true;
				}

				const expensiveCategory = activeCategory === 'high' || activeCategory === 'peak';
				return expensiveCategory && activePrice >= expensiveChargeThresholdKr;
			}

			function isVeryExpensiveSelection() {
				return activeCategory === 'peak';
			}

			function isExpensiveHoldSelection() {
				return activeCategory === 'high' || activeCategory === 'peak';
			}

			function isCheapDischargeSelection() {
				if (typeof activePrice !== 'number' || Number.isNaN(activePrice)) {
					return false;
				}

				return activeCategory === 'low' && activePrice <= cheapDischargeThresholdKr;
			}

			function formatPriceKr(priceValue) {
				if (typeof priceValue !== 'number' || Number.isNaN(priceValue)) {
					return 'ukendt pris';
				}

				return `${priceValue.toFixed(2).replace('.', ',')} kr/kWh`;
			}

			async function confirmRiskAction(options) {
				const title = options && options.title ? options.title : 'Bekræft handling';
				const text = options && options.text ? options.text : 'Er du sikker på, at du vil fortsætte med denne handling?';
				const continueLabel = options && options.continueLabel ? options.continueLabel : 'Ja, bekræft';
				const fallbackMessage = options && options.fallbackMessage ? options.fallbackMessage : text;

				if (!actionRiskConfirm || !actionRiskCancel || !actionRiskContinue || !actionRiskTitle || !actionRiskText) {
					return window.confirm(fallbackMessage);
				}

				if (actionRiskConfirmPending) {
					return actionRiskConfirmPending;
				}

				actionRiskTitle.textContent = title;
				actionRiskText.textContent = text;
				actionRiskContinue.textContent = continueLabel;
				actionRiskConfirm.hidden = false;
				actionRiskCancel.focus();

				actionRiskConfirmPending = new Promise((resolve) => {
					const onCancel = () => {
						cleanup(false);
					};
					const onContinue = () => {
						cleanup(true);
					};
					const onKeydown = (event) => {
						if (event.key === 'Escape') {
							event.preventDefault();
							cleanup(false);
						}
					};

					const cleanup = (result) => {
						actionRiskCancel.removeEventListener('click', onCancel);
						actionRiskContinue.removeEventListener('click', onContinue);
						document.removeEventListener('keydown', onKeydown);
						hideActionRiskConfirm();
						resolve(result);
					};

					actionRiskCancel.addEventListener('click', onCancel);
					actionRiskContinue.addEventListener('click', onContinue);
					document.addEventListener('keydown', onKeydown);
				});

				return actionRiskConfirmPending;
			}

			async function confirmDischargeAction() {
				const isCurrentHour = isCurrentHourSelection();
				const continueLabel = isCurrentHour ? 'Ja, start discharge' : 'Ja, bekræft discharge';
				const cheapDischarge = isCheapDischargeSelection();
				const priceLabel = formatPriceKr(activePrice);
				return confirmRiskAction({
					title: cheapDischarge ? 'Bekræft afladning ved lav pris' : 'Bekræft tvungen afladning',
					text: cheapDischarge
						? `Du er ved at vælge Discharge i en billig time (${priceLabel}). Du vil tømme batteriet, selv om strømmen er billig nu. Vil du fortsætte?`
						: 'Discharge kan tømme batteriet med op til 6 kW. Hvis huset bruger mindre, bliver resten sendt ud på nettet. Vil du fortsætte?',
					continueLabel: continueLabel,
					fallbackMessage: isCurrentHour
						? (cheapDischarge
							? `Er du sikker på, at du vil tømme batteriet nu ved lav pris (${priceLabel})?`
							: 'Discharge vil starte en tvungen afladning af batteriet med op til 6 kW. Overskudsstrøm kan blive sendt ud på nettet. Vil du fortsætte?')
						: (cheapDischarge
							? `Er du sikker på, at du vil planlægge afladning ved lav pris (${priceLabel})?`
							: 'Discharge vil planlægge en tvungen afladning af batteriet med op til 6 kW på det valgte tidspunkt. Overskudsstrøm kan blive sendt ud på nettet. Vil du fortsætte?')
				});
			}

			async function confirmChargeAtExpensivePriceAction() {
				const isCurrentHour = isCurrentHourSelection();
				const priceLabel = formatPriceKr(activePrice);
				const veryExpensive = isVeryExpensiveSelection();
				return confirmRiskAction({
					title: veryExpensive ? 'Bekræft opladning i meget dyr time' : 'Bekræft opladning ved høj pris',
					text: veryExpensive
						? `Du er ved at vælge Charge i en meget dyr time (${priceLabel}). Opladning kan trække op til 6 kW, og strømprisen er meget høj i dette tidsrum. Vil du fortsætte?`
						: `Du er ved at vælge Charge i en dyr time (${priceLabel}). Opladning kan trække op til 6 kW, og strømprisen er høj i dette tidsrum. Vil du fortsætte?`,
					continueLabel: isCurrentHour ? 'Ja, start charge' : 'Ja, bekræft charge',
					fallbackMessage: isCurrentHour
						? (veryExpensive
							? `Er du sikker på, at du vil starte opladning nu? Opladning kan trække op til 6 kW, og prisen er meget høj (${priceLabel}) i denne time.`
							: `Er du sikker på, at du vil starte opladning nu? Opladning kan trække op til 6 kW, og prisen er høj (${priceLabel}) i denne time.`)
						: (veryExpensive
							? `Er du sikker på, at du vil planlægge opladning i dette tidsrum? Opladning kan trække op til 6 kW, og prisen er meget høj (${priceLabel}) i denne time.`
							: `Er du sikker på, at du vil planlægge opladning i dette tidsrum? Opladning kan trække op til 6 kW, og prisen er høj (${priceLabel}) i denne time.`)
				});
			}

			async function confirmHoldAtExpensivePriceAction() {
				const isCurrentHour = isCurrentHourSelection();
				const priceLabel = formatPriceKr(activePrice);
				const veryExpensive = isVeryExpensiveSelection();
				return confirmRiskAction({
					title: veryExpensive ? 'Bekræft hold i meget dyr time' : 'Bekræft hold i dyr time',
					text: veryExpensive
						? `Du er ved at vælge Hold i en meget dyr time (${priceLabel}). Det betyder, at batteriet ikke aflader i denne periode, når strømprisen er meget høj. Vil du fortsætte?`
						: `Du er ved at vælge Hold i en dyr time (${priceLabel}). Det betyder, at batteriet ikke aflader i denne periode, når strømprisen er høj. Vil du fortsætte?`,
					continueLabel: isCurrentHour ? 'Ja, start hold' : 'Ja, bekræft hold',
					fallbackMessage: isCurrentHour
						? (veryExpensive
							? `Er du sikker på, at du vil sætte batteriet på Hold nu? Det betyder, at batteriet ikke aflader, når strømprisen er meget høj (${priceLabel}) i denne time.`
							: `Er du sikker på, at du vil sætte batteriet på Hold nu? Det betyder, at batteriet ikke aflader, når strømprisen er høj (${priceLabel}) i denne time.`)
						: (veryExpensive
							? `Er du sikker på, at du vil planlægge Hold i dette tidsrum? Det betyder, at batteriet ikke aflader, når strømprisen er meget høj (${priceLabel}) i denne time.`
							: `Er du sikker på, at du vil planlægge Hold i dette tidsrum? Det betyder, at batteriet ikke aflader, når strømprisen er høj (${priceLabel}) i denne time.`)
				});
			}

			function actionLabel(actionName) {
				switch ((actionName || '').toLowerCase()) {
					case 'charge': return 'Charge';
					case 'discharge': return 'Discharge';
					case 'hold': return 'Hold';
					case 'auto': return 'Auto';
					default: return 'Ukendt';
				}
			}

			function setSlotCategory(categoryName) {
				if (!slot) return;
				slot.classList.remove('low', 'medium', 'high', 'peak');
				const normalized = (categoryName || '').toLowerCase();
				if (normalized === 'low' || normalized === 'medium' || normalized === 'high' || normalized === 'peak') {
					slot.classList.add(normalized);
				}
			}

			function setCurrentActionChip(actionName) {
				if (!currentActionChip) return;
				currentActionChip.classList.remove('charge', 'discharge', 'hold', 'auto');
				const normalized = (actionName || 'auto').toLowerCase();
				if (normalized === 'charge' || normalized === 'discharge' || normalized === 'hold' || normalized === 'auto') {
					currentActionChip.classList.add(normalized);
				}
				currentActionChip.innerHTML = `<span class="chip-icon" aria-hidden="true">${actionIconSvg(normalized)}</span><span class="chip-label">${actionLabel(normalized)}</span>`;
			}

			function setStatus(message, isError) {
				if (!status) return;
				status.textContent = message || '';
				status.style.color = isError ? '#fca5a5' : '#cbd5e1';
			}

			function setSelectedAction(nextAction) {
				activeAction = (nextAction || 'auto').trim().toLowerCase();
				choiceButtons.forEach((btn) => {
					const actionName = (btn.getAttribute('data-action') || '').trim().toLowerCase();
					const isSelected = actionName === activeAction;
					const isCurrent = actionName === currentAction;
					btn.classList.toggle('is-selected', isSelected);
					btn.classList.toggle('is-current', isCurrent);
					btn.setAttribute('aria-checked', isSelected ? 'true' : 'false');
					btn.tabIndex = (isCurrent || isSaving) ? -1 : 0;
					btn.disabled = isCurrent || isSaving;
					btn.setAttribute('aria-disabled', (isCurrent || isSaving) ? 'true' : 'false');
				});
			}

			function updateActionChoicesLayout() {
				choiceButtons.forEach((btn) => btn.classList.remove('is-full-width'));
			}

			function showLoginPanel() {
				if (loginPanel) loginPanel.style.display = '';
				if (editorPanel) editorPanel.style.display = 'none';
				if (loginPassword) {
					window.setTimeout(() => {
						loginPassword.focus();
						loginPassword.select();
					}, 0);
				}
			}

			function showEditorPanel() {
				if (loginPanel) loginPanel.style.display = 'none';
				if (editorPanel) editorPanel.style.display = '';
			}

			async function requestAuthStatus() {
				const res = await fetch('?action=planning-auth-status', { cache: 'no-store' });
				if (!res.ok) return false;
				const data = await res.json();
				if (data && typeof window.powerBuddyApplyBatteryLinkTarget === 'function') {
					window.powerBuddyApplyBatteryLinkTarget(data.battery_link || '');
				}
				return !!(data && data.authorized);
			}

			async function refreshAuthPanel() {
				setStatus('', false);
				try {
					const authorized = await requestAuthStatus();
					if (authorized) {
						showEditorPanel();
						setStatus('Tryk på en handling for at gemme med det samme.', false);
					} else {
						showLoginPanel();
						setStatus('Log ind for at ændre handling for dette tidspunkt.', false);
					}
				} catch (_) {
					showLoginPanel();
					setStatus('Kunne ikke tjekke login status.', true);
				}
			}

			function openModal(actionId, startTime, actionLabel, currentActionName, categoryName, priceValue) {
				modalMode = 'action';
				pendingControlCommand = '';
				hideActionRiskConfirm();
				activeActionId = actionId || '';
				activeStartTime = startTime || '';
				activeCategory = (categoryName || '').toLowerCase();
				activePrice = Number.isFinite(priceValue) ? priceValue : null;
				currentAction = (currentActionName || 'auto').trim().toLowerCase();
				setSelectedAction(currentAction);
				setCurrentActionChip(currentAction);
				if (slot) {
					slot.textContent = `Tidspunkt: ${actionLabel || '-'}`;
				}
				setSlotCategory(activeCategory);
				updateActionChoicesLayout();
				modal.classList.add('open');
				modal.setAttribute('aria-hidden', 'false');
				updateBodyScrollLock();
				refreshAuthPanel();
			}

			function closeModal() {
				hideActionRiskConfirm();
				modalMode = 'action';
				pendingControlCommand = '';
				modal.classList.remove('open');
				modal.setAttribute('aria-hidden', 'true');
				updateBodyScrollLock();
				setSlotCategory('');
				setStatus('', false);
				const activeEl = document.activeElement;
				if (activeEl && activeEl instanceof HTMLElement && activeEl.classList.contains('bar-wrapper')) {
					activeEl.blur();
				}
			}

			actionTargets.forEach((target) => {
				target.tabIndex = 0;
				target.setAttribute('role', 'button');
				target.setAttribute('aria-label', `Rediger handling for ${target.getAttribute('data-hour-label') || 'valgt time'}`);

				const openFromTarget = () => {
					const actionId = target.getAttribute('data-action-id') || '';
					const startTime = target.getAttribute('data-start-time') || '';
					const actionLabel = target.getAttribute('data-hour-label') || '';
					const currentAction = (target.getAttribute('data-action') || 'auto').toLowerCase();
					const category = (target.getAttribute('data-category') || '').toLowerCase();
					const priceRaw = target.getAttribute('data-price') || '';
					const priceValue = Number.parseFloat(priceRaw);
					openModal(actionId, startTime, actionLabel, currentAction, category, priceValue);
				};

				target.addEventListener('click', openFromTarget);
				target.addEventListener('keydown', (event) => {
					if (event.key === 'Enter' || event.key === ' ') {
						event.preventDefault();
						openFromTarget();
					}
				});
			});

			choiceButtons.forEach((btn) => {
				btn.addEventListener('click', async () => {
					if (isSaving) return;
					const nextAction = (btn.getAttribute('data-action') || 'auto').toLowerCase();
					if (nextAction === currentAction) {
						return;
					}

					if (nextAction === 'discharge') {
						const confirmDischarge = await confirmDischargeAction();
						if (!confirmDischarge) {
							return;
						}
					}

					if (nextAction === 'charge' && isExpensiveSelection()) {
						const confirmCharge = await confirmChargeAtExpensivePriceAction();
						if (!confirmCharge) {
							return;
						}
					}

					if (nextAction === 'hold' && isExpensiveHoldSelection()) {
						const confirmHold = await confirmHoldAtExpensivePriceAction();
						if (!confirmHold) {
							return;
						}
					}

					setSelectedAction(nextAction);
					isSaving = true;
					setSelectedAction(nextAction);
					setStatus('Gemmer ændring...', false);

					let saved = false;
					try {
						const body = new URLSearchParams();
						if (activeActionId) body.set('actionId', activeActionId);
						if (activeStartTime) body.set('startTime', activeStartTime);
						body.set('actionName', nextAction);
						const res = await fetch('?action=planning-update', {
							method: 'POST',
							headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
							body: body.toString()
						});
						const data = await res.json();
						if (res.ok && data && data.ok) {
							saved = true;
							closeModal();
							persistChartScrollForReload();
							const reloadUrl = new URL(window.location.href);
							reloadUrl.searchParams.set('pbRefresh', String(Date.now()));
							window.location.replace(reloadUrl.toString());
							return;
						}
						if (data && data.error === 'unauthorized') {
							showLoginPanel();
							setStatus('Session udløbet. Log ind igen.', true);
							return;
						}
						const errCode = (data && (data.error || data.statusCode || data.message)) ? ` (${data.error || data.statusCode || data.message})` : '';
						setStatus(`Kunne ikke gemme handling${errCode}.`, true);
					} catch (_) {
						setStatus('Kunne ikke gemme handling.', true);
					} finally {
						if (!saved) {
							activeAction = currentAction;
						}
						isSaving = false;
						setSelectedAction(activeAction);
					}
				});
			});

			if (loginForm) {
				loginForm.addEventListener('submit', async (event) => {
					event.preventDefault();
					if (!loginPassword || !loginSubmit) return;

					const password = loginPassword.value.trim();
					if (!password) {
						setStatus('Indtast adgangskode.', true);
						return;
					}

					loginSubmit.disabled = true;
					setStatus('Logger ind...', false);
					try {
						const body = new URLSearchParams();
						body.set('password', password);
						body.set('txtLogin', password);
						const loginUrl = new URL(window.location.href);
						loginUrl.pathname = loginUrl.pathname.replace(/\/+$/, '') || '/';
						loginUrl.search = '?action=planning-auth-login';
						const res = await fetch(loginUrl.toString(), {
							method: 'POST',
							headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
							body: body.toString()
						});
						const data = await res.json();
						if (res.ok && data && data.ok && data.authorized) {
							if (typeof window.powerBuddyApplyBatteryLinkTarget === 'function') {
								window.powerBuddyApplyBatteryLinkTarget(data.battery_link || '');
							}
							loginPassword.value = '';
							if (modalMode === 'control-login' && pendingControlCommand) {
								const commandToRun = pendingControlCommand;
								closeModal();
								if (commandToRun === 'pause') {
									const confirmed = await confirmPausePowerBuddyAction();
									if (!confirmed) {
										return;
									}
								}
								await executePowerBuddyControl(commandToRun);
								return;
							}
							showEditorPanel();
							setSelectedAction(currentAction);
							setStatus('Login godkendt. Tryk på en handling for at gemme.', false);
						} else if (res.status === 401) {
							setStatus('Forkert adgangskode.', true);
						} else {
							setStatus('Login kunne ikke gennemføres. Prøv igen.', true);
						}
					} catch (_) {
						setStatus('Login fejlede. Prøv igen.', true);
					} finally {
						loginSubmit.disabled = false;
					}
				});
			}

			if (closeBtn) {
				closeBtn.addEventListener('click', closeModal);
			}

			if (chartRunToggle) {
				chartRunToggle.addEventListener('click', async () => {
					const command = (chartRunToggle.getAttribute('data-command') || 'pause').toLowerCase();
					await requestControl(command);
				});
			}

			if (chartStartBtn) {
				chartStartBtn.addEventListener('click', async () => {
					const command = (chartStartBtn.getAttribute('data-command') || 'start').toLowerCase();
					await requestControl(command);
				});
			}

			modal.addEventListener('click', (event) => {
				if (event.target === modal) {
					closeModal();
				}
			});

			setPowerBuddyPausedUi(isPowerBuddyPaused);

			document.addEventListener('keydown', (event) => {
				if (event.key === 'Escape' && modal.classList.contains('open')) {
					closeModal();
				}
			});
		})();

		(function () {
			let refreshTimerId = null;

			function getMsUntilNextHourlyRefresh() {
				const now = new Date();
				const next = new Date(now);
				next.setMinutes(0, 2, 0);

				if (next <= now) {
					next.setHours(next.getHours() + 1);
				}

				return Math.max(0, next.getTime() - now.getTime());
			}

			function scheduleNextHourlyRefresh() {
				if (refreshTimerId) {
					window.clearTimeout(refreshTimerId);
				}

				const delayMs = getMsUntilNextHourlyRefresh();
				refreshTimerId = window.setTimeout(() => {
					window.location.reload();
				}, delayMs);
			}

			scheduleNextHourlyRefresh();

			document.addEventListener('visibilitychange', () => {
				if (!document.hidden) {
					scheduleNextHourlyRefresh();
				}
			});
		})();

		(function () {
			const menuButton = document.getElementById('pbHeaderMenuButton');
			const menuPanel = document.getElementById('pbMenuPanel');
			const menuIcon = document.getElementById('pbHeaderMenuIcon');
			const logModal = document.getElementById('pbLogModal');
			const logOutput = document.getElementById('pbLogOutput');
			const logStatus = document.getElementById('pbLogStatus');
			const logClose = document.getElementById('pbLogModalClose');
			const logFilterButtons = Array.from(document.querySelectorAll('[data-log-filter]'));
			const logSearchInput = document.getElementById('pbLogSearch');
			const headerLoginModal = document.getElementById('pbHeaderLoginModal');
			const headerLoginForm = document.getElementById('pbHeaderLoginForm');
			const headerLoginPassword = document.getElementById('pbHeaderLoginPassword');
			const headerLoginSubmit = document.getElementById('pbHeaderLoginSubmit');
			const headerLoginStatus = document.getElementById('pbHeaderLoginStatus');
			const headerLoginClose = document.getElementById('pbHeaderLoginClose');
			const settingsModal = document.getElementById('pbSettingsModal');
			const settingsClose = document.getElementById('pbSettingsModalClose');
			const settingsStatus = document.getElementById('pbSettingsStatus');
			const settingsActions = Array.from(document.querySelectorAll('[data-settings-action]'));
			const dummyPricesToggle = document.getElementById('pbDummyPricesToggle');
			let isAuthorized = false;

			function setMenuState(authorized) {
				isAuthorized = !!authorized;
				if (!menuButton) return;
				menuButton.classList.toggle('is-locked', !isAuthorized);
				menuButton.setAttribute('aria-label', isAuthorized ? 'Menu' : 'Log ind');
				if (!menuIcon) return;
				if (isAuthorized) {
					menuIcon.innerHTML = '<span class="pb-menu-button-lines"><span></span><span></span><span></span></span>';
				} else {
					menuIcon.textContent = '🔒';
				}
			}

			function closeMenuPanel() {
				if (!menuPanel) return;
				menuPanel.classList.remove('open');
				menuPanel.setAttribute('aria-hidden', 'true');
				if (menuButton) {
					menuButton.setAttribute('aria-expanded', 'false');
				}
			}

			function openMenuPanel() {
				if (!menuPanel || !isAuthorized) return;
				menuPanel.classList.add('open');
				menuPanel.setAttribute('aria-hidden', 'false');
				if (menuButton) {
					menuButton.setAttribute('aria-expanded', 'true');
				}
			}

			function openLoginModal() {
				if (!headerLoginModal) return;
				headerLoginModal.classList.add('open');
				headerLoginModal.setAttribute('aria-hidden', 'false');
				document.body.classList.add('pb-modal-open');
				if (headerLoginPassword) {
					window.setTimeout(() => {
						headerLoginPassword.focus();
						headerLoginPassword.select();
					}, 0);
				}
			}

			function closeLoginModal() {
				if (!headerLoginModal) return;
				headerLoginModal.classList.remove('open');
				headerLoginModal.setAttribute('aria-hidden', 'true');
				if (!document.querySelector('.pb-log-modal.open, .pb-action-modal.open')) {
					document.body.classList.remove('pb-modal-open');
				}
				if (headerLoginStatus) {
					headerLoginStatus.textContent = '';
				}
				if (headerLoginPassword) {
					headerLoginPassword.value = '';
				}
			}

			function openSettingsModal() {
				if (!settingsModal) return;
				settingsModal.classList.add('open');
				settingsModal.setAttribute('aria-hidden', 'false');
				document.body.classList.add('pb-modal-open');
				if (settingsStatus) settingsStatus.textContent = '';
				loadDummyPriceSetting();
			}

			function settingsActionUrl() {
				const currentUrl = new URL(window.location.href);
				const basePath = currentUrl.pathname.replace(/\/+$/, '') || '/';
				return new URL('settings/action', `${currentUrl.origin}${basePath}/`);
			}

			async function postSettingsAction(payload) {
				const res = await fetch(settingsActionUrl().toString(), {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify(payload),
				});
				const data = await res.json().catch(() => null);
				if (!res.ok || !data || !data.ok) {
					const detail = data && (data.detail || data.error);
					throw new Error(detail || (res.status === 401 ? 'unauthorized' : 'settings_failed'));
				}
				return data;
			}

			async function loadDummyPriceSetting() {
				if (!dummyPricesToggle || !isAuthorized) return;
				dummyPricesToggle.disabled = true;
				try {
					const data = await postSettingsAction({ action: 'dummy-prices-status' });
					dummyPricesToggle.checked = Boolean(data.enabled);
				} catch (error) {
					if (settingsStatus) settingsStatus.textContent = error && error.message === 'unauthorized'
						? 'Sessionen er udløbet. Log ind igen.'
						: `Indstillingen kunne ikke hentes: ${error && error.message ? error.message : 'ukendt fejl'}.`;
				} finally {
					dummyPricesToggle.disabled = false;
				}
			}

			function closeSettingsModal() {
				if (!settingsModal) return;
				settingsModal.classList.remove('open');
				settingsModal.setAttribute('aria-hidden', 'true');
				if (!document.querySelector('.pb-log-modal.open, .pb-action-modal.open')) {
					document.body.classList.remove('pb-modal-open');
				}
			}

			let activeLogFilter = 'all';

			function cleanLogLines(lines) {
				return (Array.isArray(lines) ? lines : []).filter((line) => {
					if (!String(line || '').trim()) return false;
					const parsed = parseLogLine(line);
					return !parsed || normalizeLogLevel(parsed.level) !== 'debug';
				});
			}

			function normalizeLogLevel(levelName) {
				const value = String(levelName || '').trim().toLowerCase();
				if (!value) return 'info';
				if (value === 'warn') return 'warning';
				if (value === 'crit' || value === 'critical') return 'error';
				if (value === 'err') return 'error';
				if (value === 'info') return 'info';
				if (value === 'debug') return 'debug';
				if (value === 'warning') return 'warning';
				if (value === 'error') return 'error';
				return 'info';
			}

			function formatDisplayTimestamp(rawTimestamp) {
				if (!rawTimestamp) return '—';
				const isoMatch = String(rawTimestamp).match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d+))?)?/);
				if (isoMatch) {
					return `${isoMatch[3]}-${isoMatch[2]}-${isoMatch[1]} ${isoMatch[4]}:${isoMatch[5]}`;
				}
				const legacyMatch = String(rawTimestamp).match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
				if (legacyMatch) {
					return `${legacyMatch[3]}-${legacyMatch[2]}-${legacyMatch[1]} ${legacyMatch[4]}:${legacyMatch[5]}`;
				}
				return String(rawTimestamp).replace(',', ' ').replace('T', ' ');
			}

			function parseLogLine(rawLine) {
				const line = String(rawLine || '').trimEnd();
				if (!line) {
					return null;
				}

				const match = line.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+(.*)$/i);
				if (match) {
					return {
						timestamp: match[1],
						level: normalizeLogLevel(match[2]),
						message: match[3].trim(),
					};
				}

				const fallbackMatch = line.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(\w+)\s+(.*)$/i);
				if (fallbackMatch) {
					return {
						timestamp: fallbackMatch[1],
						level: normalizeLogLevel(fallbackMatch[2]),
						message: fallbackMatch[3].trim(),
					};
				}

				const fallbackText = line.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$/i);
				if (fallbackText) {
					return {
						timestamp: `${fallbackText[1]} ${fallbackText[2]}`,
						level: normalizeLogLevel(fallbackText[3]),
						message: fallbackText[4].trim(),
					};
				}

				return {
					timestamp: '',
					level: 'info',
					message: line,
				};
			}

			function renderLogRows(lines) {
				if (!logOutput) return;
				const entries = cleanLogLines(lines);
				logOutput.innerHTML = '';
				if (!entries.length) {
					logOutput.innerHTML = '<div class="pb-log-line"><div class="pb-log-message">Ingen logposter matcher filteret.</div></div>';
					return;
				}

				entries.forEach((entry) => {
					const row = document.createElement('div');
					row.className = 'pb-log-line';
					const parsed = parseLogLine(entry);
					const levelName = normalizeLogLevel((parsed && parsed.level) || 'info');
					const levelClass = levelName === 'error' ? 'error' : levelName === 'warning' ? 'warning' : levelName === 'debug' ? 'debug' : 'info';
					row.classList.add(`is-${levelClass}`);

					const meta = document.createElement('div');
					meta.className = 'pb-log-meta';

					const ts = document.createElement('div');
					ts.className = 'pb-log-ts';
					ts.textContent = formatDisplayTimestamp(parsed && parsed.timestamp ? parsed.timestamp : '');

					const badge = document.createElement('div');
					badge.className = `pb-log-level ${levelClass}`;
					badge.textContent = levelName;

					const message = document.createElement('div');
					message.className = 'pb-log-message';
					message.textContent = parsed ? parsed.message : String(entry);

					meta.append(ts, badge);
					row.append(meta, message);
					logOutput.appendChild(row);
				});
			}

			function setLogFilter(nextFilter) {
				activeLogFilter = nextFilter || 'all';
				const rawLines = cleanLogLines(String(logOutput && logOutput.dataset.rawText || '').split(/\n/));
				const query = (logSearchInput ? logSearchInput.value.trim().toLowerCase() : '');
				const matchingLines = rawLines.filter((line) => !query || String(line).toLowerCase().includes(query));
				const counts = { all: matchingLines.length, error: 0, warning: 0, info: 0, debug: 0 };
				matchingLines.forEach((line) => {
					const level = normalizeLogLevel((parseLogLine(line) || {}).level);
					if (Object.prototype.hasOwnProperty.call(counts, level)) counts[level] += 1;
				});
				if (activeLogFilter !== 'all' && counts[activeLogFilter] === 0) activeLogFilter = 'all';
				logFilterButtons.forEach((button) => {
					const filter = button.getAttribute('data-log-filter') || 'all';
					const visible = counts[filter] > 0;
					button.hidden = !visible;
					button.setAttribute('aria-hidden', visible ? 'false' : 'true');
					const matches = filter === activeLogFilter;
					button.classList.toggle('is-active', matches);
					button.setAttribute('aria-pressed', matches ? 'true' : 'false');
				});
				if (logOutput && rawLines.length) {
					const filtered = rawLines.filter((line) => {
						if (query && !String(line).toLowerCase().includes(query)) return false;
						const parsed = parseLogLine(line);
						const normalized = normalizeLogLevel(parsed ? parsed.level : 'info');
						return activeLogFilter === 'all' || normalized === activeLogFilter;
					});
					renderLogRows(filtered.slice().reverse());
				}
			}

			function setLogUnavailableState(message = 'Loggen er ikke tilgængelig. Log ind for at se data.') {
				if (logOutput) {
					logOutput.dataset.rawText = '';
					logOutput.innerHTML = '';
					const messageNode = document.createElement('div');
					messageNode.className = 'pb-log-message';
					messageNode.textContent = message;
					logOutput.appendChild(messageNode);
				}
				logFilterButtons.forEach((button) => {
					button.hidden = true;
					button.setAttribute('aria-hidden', 'true');
				});
				if (logStatus) {
					logStatus.textContent = 'Du skal være logget ind for at se loggen.';
				}
			}

			async function openLogModal() {
				if (!logModal) return;
				logModal.classList.add('open');
				logModal.setAttribute('aria-hidden', 'false');
				document.body.classList.add('pb-modal-open');
				if (logStatus) {
					logStatus.textContent = 'Kontrollerer login...';
				}
				if (logOutput) {
					logOutput.textContent = 'Kontrollerer login...';
				}

				const authOk = await refreshAuthState();
				if (!authOk) {
					setLogUnavailableState();
					return;
				}

				if (logStatus) {
					logStatus.textContent = 'Henter log...';
				}
				if (logOutput) {
					logOutput.textContent = 'Henter log...';
				}

				try {
					const currentUrl = new URL(window.location.href);
					const basePath = currentUrl.pathname.replace(/\/+$/, '') || '/';
					const logUrl = new URL('dashboard/logs', `${currentUrl.origin}${basePath}/`);
					logUrl.searchParams.set('lines', '250');
					logUrl.searchParams.set('ts', String(Date.now()));
					const res = await fetch(logUrl.toString(), { cache: 'no-store' });
					if (!res.ok) {
						throw new Error('unauthorized');
					}
					const data = await res.json();
					const lines = Array.isArray(data && data.lines) ? data.lines : [];
					const rawText = cleanLogLines(lines).join('');
					if (logOutput) {
						logOutput.dataset.rawText = rawText;
						const rows = rawText ? rawText.split(/\n/).filter((entry) => entry.trim()) : [];
						renderLogRows(rows.slice().reverse());
						if (!rows.length) {
							logOutput.textContent = 'Ingen logdata tilgængelig.';
						}
					}
					if (logStatus) logStatus.textContent = data && data.path ? `Logfil: ${data.path}` : 'Log hentet.';
					setLogFilter(activeLogFilter);
				} catch (_) {
					setLogUnavailableState();
				}
			}

			function closeLogModal() {
				if (!logModal) return;
				logModal.classList.remove('open');
				logModal.setAttribute('aria-hidden', 'true');
				if (!document.querySelector('.pb-log-modal.open, .pb-action-modal.open')) {
					document.body.classList.remove('pb-modal-open');
				}
			}

			async function refreshAuthState() {
				try {
					const res = await fetch('?action=planning-auth-status', { cache: 'no-store' });
					if (!res.ok) {
						setMenuState(false);
						return false;
					}
					const data = await res.json();
					const authorized = Boolean(data && data.authorized);
					setMenuState(authorized);
					return authorized;
				} catch (_) {
					setMenuState(false);
					return false;
				}
			}

			if (menuButton) {
				menuButton.addEventListener('click', async () => {
					if (!isAuthorized) {
						openLoginModal();
						return;
					}
					if (menuPanel && menuPanel.classList.contains('open')) {
						closeMenuPanel();
					} else {
						openMenuPanel();
					}
				});
			}

			logFilterButtons.forEach((button) => {
				button.addEventListener('click', () => {
					setLogFilter(button.getAttribute('data-log-filter') || 'all');
				});
			});

			if (logSearchInput) {
				logSearchInput.addEventListener('input', () => {
					if (logOutput && logOutput.dataset.rawText) {
						setLogFilter(activeLogFilter);
					}
				});
			}

			settingsActions.forEach((button) => {
				button.addEventListener('click', async () => {
					const action = button.getAttribute('data-settings-action') || '';
					if (!action || !isAuthorized) return;
					settingsActions.forEach((item) => { item.disabled = true; });
					if (settingsStatus) settingsStatus.textContent = 'Kører handling...';
					try {
						await postSettingsAction({ action });
						if (settingsStatus) settingsStatus.textContent = 'Handling gennemført. Opdaterer...';
						window.setTimeout(() => window.location.reload(), 250);
					} catch (error) {
						if (settingsStatus) {
							settingsStatus.textContent = error && error.message === 'unauthorized'
								? 'Sessionen er udløbet. Log ind igen.'
								: 'Handlingen kunne ikke gennemføres. Prøv igen.';
						}
						settingsActions.forEach((item) => { item.disabled = false; });
					}
				});
			});

			if (dummyPricesToggle) {
				dummyPricesToggle.addEventListener('change', async () => {
					if (!isAuthorized) return;
					const enabled = dummyPricesToggle.checked;
					dummyPricesToggle.disabled = true;
					try {
						const data = await postSettingsAction({ action: 'set-dummy-prices', enabled });
						dummyPricesToggle.checked = Boolean(data.enabled);
						if (settingsStatus) settingsStatus.textContent = '';
					} catch (error) {
						dummyPricesToggle.checked = !enabled;
						if (settingsStatus) settingsStatus.textContent = error && error.message === 'unauthorized'
							? 'Sessionen er udløbet. Log ind igen.'
							: `Indstillingen kunne ikke gemmes: ${error && error.message ? error.message : 'ukendt fejl'}.`;
					} finally {
						dummyPricesToggle.disabled = false;
					}
				});
			}

			Array.from(document.querySelectorAll('[data-menu-action]')).forEach((item) => {
				item.addEventListener('click', () => {
					const action = item.getAttribute('data-menu-action');
					closeMenuPanel();
					if (action === 'logs') {
						openLogModal();
					}
					if (action === 'settings') {
						openSettingsModal();
					}
				});
			});

			if (logClose) {
				logClose.addEventListener('click', closeLogModal);
			}
			if (logModal) {
				logModal.addEventListener('click', (event) => {
					if (event.target === logModal) closeLogModal();
				});
			}
			if (settingsClose) settingsClose.addEventListener('click', closeSettingsModal);
			if (settingsModal) {
				settingsModal.addEventListener('click', (event) => {
					if (event.target === settingsModal) closeSettingsModal();
				});
			}
			if (headerLoginClose) {
				headerLoginClose.addEventListener('click', closeLoginModal);
			}
			if (headerLoginModal) {
				headerLoginModal.addEventListener('click', (event) => {
					if (event.target === headerLoginModal) closeLoginModal();
				});
			}
			if (headerLoginForm) {
				headerLoginForm.addEventListener('submit', async (event) => {
					event.preventDefault();
					if (!headerLoginPassword || !headerLoginSubmit) return;
					const password = headerLoginPassword.value.trim();
					if (!password) {
						if (headerLoginStatus) headerLoginStatus.textContent = 'Indtast adgangskode.';
						return;
					}
					headerLoginSubmit.disabled = true;
					if (headerLoginStatus) headerLoginStatus.textContent = 'Logger ind...';
					const body = new URLSearchParams();
					body.set('password', password);
					body.set('txtLogin', password);
					try {
						const loginUrl = new URL(window.location.href);
						loginUrl.pathname = loginUrl.pathname.replace(/\/+$/, '') || '/';
						loginUrl.search = '?action=planning-auth-login';
						const res = await fetch(loginUrl.toString(), {
							method: 'POST',
							headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
							body: body.toString()
						});
						const data = await res.json();
						if (res.ok && data && data.ok && data.authorized) {
							setMenuState(true);
							closeLoginModal();
							openMenuPanel();
							return;
						}
						if (headerLoginStatus) {
							headerLoginStatus.textContent = res.status === 401
								? 'Forkert adgangskode.'
								: 'Login kunne ikke gennemføres. Prøv igen.';
						}
					} catch (_) {
						if (headerLoginStatus) headerLoginStatus.textContent = 'Login fejlede. Prøv igen.';
					} finally {
						headerLoginSubmit.disabled = false;
					}
				});
			}

			refreshAuthState();
		})();

		(function () {
			if (!('ontouchstart' in window) || !window.matchMedia('(pointer: coarse)').matches) {
				return;
			}

			const pullThresholdPx = 90;
			const indicatorMaxTravelPx = 72;
			const indicator = document.getElementById('pullRefreshIndicator');
			const indicatorText = document.getElementById('pullRefreshText');
			let isPullingFromTop = false;
			let pullRefreshBlocked = false;
			let startY = 0;
			let maxPull = 0;

			function isWithinHorizontallyScrollableChart(target) {
				if (!(target instanceof Element)) {
					return false;
				}

				const scrollEl = target.closest('.chart-scroll');
				if (!scrollEl) {
					return false;
				}

				return scrollEl.scrollWidth > (scrollEl.clientWidth + 1);
			}

			function isInteractionInsideModal(target) {
				if (!(target instanceof Element)) {
					return false;
				}

				if (target.closest('.pb-log-modal.open, .pb-menu-panel.open')) {
					return true;
				}

				const openModal = document.querySelector('.pb-log-modal.open');
				const openMenu = document.querySelector('.pb-menu-panel.open');
				if (!openModal && !openMenu) {
					return false;
				}

				return !!target.closest('.pb-log-modal, .pb-menu-panel');
			}

			function setIndicatorState(pullPx) {
				if (!indicator || !indicatorText) {
					return;
				}

				if (pullPx <= 0) {
					indicator.classList.remove('visible', 'ready');
					indicator.style.transform = 'translate(-50%, -140%)';
					indicatorText.textContent = 'Træk ned for at opdatere';
					return;
				}

				const travel = Math.min(indicatorMaxTravelPx, pullPx * 0.58);
				const isReady = pullPx >= pullThresholdPx;
				indicator.classList.add('visible');
				indicator.classList.toggle('ready', isReady);
				indicator.style.transform = `translate(-50%, calc(-140% + ${travel}px))`;
				indicatorText.textContent = isReady ? 'Slip for at opdatere' : 'Træk ned for at opdatere';
			}

			window.addEventListener('touchstart', (event) => {
				pullRefreshBlocked = isInteractionInsideModal(event.target);
				if (pullRefreshBlocked) {
					isPullingFromTop = false;
					setIndicatorState(0);
					return;
				}

				if (event.touches.length === 1 && isWithinHorizontallyScrollableChart(event.target)) {
					isPullingFromTop = false;
					setIndicatorState(0);
					return;
				}

				if (event.touches.length !== 1 || window.scrollY > 0) {
					isPullingFromTop = false;
					setIndicatorState(0);
					return;
				}

				isPullingFromTop = true;
				startY = event.touches[0].clientY;
				maxPull = 0;
				setIndicatorState(0);
			}, { passive: true });

			window.addEventListener('touchmove', (event) => {
				if (pullRefreshBlocked || isInteractionInsideModal(event.target)) {
					isPullingFromTop = false;
					setIndicatorState(0);
					return;
				}

				if (!isPullingFromTop || event.touches.length !== 1) {
					return;
				}

				const deltaY = event.touches[0].clientY - startY;
				if (deltaY <= 0) {
					isPullingFromTop = false;
					setIndicatorState(0);
					return;
				}

				if (deltaY > maxPull) {
					maxPull = deltaY;
				}

				setIndicatorState(deltaY);
			}, { passive: true });

			window.addEventListener('touchend', () => {
				if (isPullingFromTop && maxPull >= pullThresholdPx) {
					if (indicatorText) {
						indicatorText.textContent = 'Opdaterer...';
					}
					window.setTimeout(() => {
						window.location.reload();
					}, 120);
					return;
				}

				setIndicatorState(0);
				isPullingFromTop = false;
				pullRefreshBlocked = false;
				startY = 0;
				maxPull = 0;
			}, { passive: true });

			window.addEventListener('touchcancel', () => {
				setIndicatorState(0);
				isPullingFromTop = false;
				pullRefreshBlocked = false;
				startY = 0;
				maxPull = 0;
			}, { passive: true });
		})();

		(function () {
			const logoRefresh = document.getElementById('logoRefresh');
			if (!logoRefresh) return;

			const refreshPage = () => {
				window.location.reload();
			};

			logoRefresh.addEventListener('click', refreshPage);
			logoRefresh.addEventListener('keydown', (event) => {
				if (event.key === 'Enter' || event.key === ' ') {
					event.preventDefault();
					refreshPage();
				}
			});
		})();

		(function () {
			const round1 = (n) => Math.round(n * 10) / 10;
			const toDa = (n) => round1(n).toFixed(1).replace('.', ',');
			// Formatér watt: under 1000 W vises som "XXX W", ellers "X,Y kW"
			const formatPower = (w) => {
				const absW = Math.abs(w);
				if (absW < 1000) return `${Math.round(absW)} W`;
				return `${toDa(absW / 1000)} kW`;
			};
			const asNum = (v) => {
				const n = Number(v);
				return Number.isFinite(n) ? n : 0;
			};
			const defaultReserveMinSoc = 5;

			let batteryDisplayMode = 'percent';
			let latestBatterySoc = 0;
			let latestBatteryEtaMinutes = null;
			let latestBatteryFlowText = 'Standby';
			let latestBatteryClockLabel = '';
			let latestBatteryPowerW = 0;
			let latestBatteryCapacityKwh = null;

			function formatEtaMinutes(totalMinutes) {
				const safeMinutes = Math.max(0, Math.round(totalMinutes));
				const hours = Math.floor(safeMinutes / 60);
				const minutes = safeMinutes % 60;
				return `${hours} t ${minutes} m`;
			}

			function formatClockFromNow(totalMinutes) {
				if (!Number.isFinite(totalMinutes) || totalMinutes < 0) return null;
				const safeMinutes = Math.max(0, Math.round(totalMinutes));
				const target = new Date(Date.now() + (safeMinutes * 60000));
				const hh = String(target.getHours()).padStart(2, '0');
				const mm = String(target.getMinutes()).padStart(2, '0');
				const dayOffset = safeMinutes > 1440 ? Math.floor(safeMinutes / 1440) : 0;
				const dayOffsetText = dayOffset === 0 ? '' : ` ${dayOffset > 0 ? `+${dayOffset}` : String(dayOffset)}`;
				return `${hh}:${mm}${dayOffsetText}`;
			}

			function pickNested(obj, path) {
				let cur = obj;
				for (let i = 0; i < path.length; i += 1) {
					if (!cur || typeof cur !== 'object' || !(path[i] in cur)) {
						return null;
					}
					cur = cur[path[i]];
				}
				return cur;
			}

			function normalizeCapacityToKwh(rawValue) {
				const value = asNum(rawValue);
				if (!Number.isFinite(value) || value <= 0) return null;
				if (value > 200) {
					return value / 1000;
				}
				return value;
			}

			function resolveBatteryCapacityKwh(data) {
				const directCandidates = [
					data && data.battery_capacity_kwh,
					data && data.battery_capacity_wh,
					data && data.battery_capacity,
					data && data.battery_capacity_maximum,
					data && data.capacity_maximum,
					data && data.designed_capacity,
					data && data.capacity
				];

				for (let i = 0; i < directCandidates.length; i += 1) {
					const normalized = normalizeCapacityToKwh(directCandidates[i]);
					if (normalized) return normalized;
				}

				const froniusController =
					pickNested(data, ['Body', 'Data', '0', 'Controller']) ||
					pickNested(data, ['body', 'data', '0', 'controller']);
				if (froniusController) {
					const froniusCandidates = [
						froniusController.Capacity_Maximum,
						froniusController.DesignedCapacity
					];
					for (let i = 0; i < froniusCandidates.length; i += 1) {
						const normalized = normalizeCapacityToKwh(froniusCandidates[i]);
						if (normalized) return normalized;
					}
				}

				return null;
			}

			function resolveReserveMinSoc(data) {
				const candidates = [
					data && data.reserve_min_soc,
					data && data.reserve_soc,
					data && data.min_soc,
					data && data.battery_min_soc,
					data && data.battery_reserve_soc
				];
				for (let i = 0; i < candidates.length; i += 1) {
					const value = asNum(candidates[i]);
					if (value >= 0 && value <= 95) return value;
				}
				return defaultReserveMinSoc;
			}

			function computeEtaMinutesFromPower(soc, batteryPowerW, batteryCapacityKwh, reserveMinSoc) {
				if (!Number.isFinite(batteryCapacityKwh) || batteryCapacityKwh <= 0) {
					return null;
				}

				const absPowerW = Math.abs(batteryPowerW);
				if (!Number.isFinite(absPowerW) || absPowerW < 50) {
					return null;
				}

				const powerKw = absPowerW / 1000;
				if (powerKw <= 0) {
					return null;
				}

				if (batteryPowerW < -10 && soc < 100) {
					const energyToFullKwh = ((100 - soc) / 100) * batteryCapacityKwh;
					const hoursToFull = energyToFullKwh / powerKw;
					const minutesToFull = hoursToFull * 60;
					return Number.isFinite(minutesToFull) && minutesToFull >= 0 ? minutesToFull : null;
				}

				if (batteryPowerW > 10 && soc > reserveMinSoc) {
					const energyToReserveKwh = ((soc - reserveMinSoc) / 100) * batteryCapacityKwh;
					const hoursToReserve = energyToReserveKwh / powerKw;
					const minutesToReserve = hoursToReserve * 60;
					return Number.isFinite(minutesToReserve) && minutesToReserve >= 0 ? minutesToReserve : null;
				}

				return null;
			}

			function getBatteryPrimaryDisplayText() {
				if (batteryDisplayMode === 'percent') {
					return `${Math.round(latestBatterySoc)}%`;
				}

				if (batteryDisplayMode === 'power-capacity') {
					if (latestBatteryCapacityKwh !== null) {
						const storedKwh = Math.max(0, (latestBatteryCapacityKwh * latestBatterySoc) / 100);
						return `${toDa(storedKwh)} kWh`;
					}
					return `${Math.round(latestBatterySoc)}%`;
				}

				if (latestBatteryEtaMinutes === null) {
					return 'Tid -';
				}

				return formatEtaMinutes(latestBatteryEtaMinutes);
			}

			function renderBatteryPrimaryValue() {
				const displayValue = getBatteryPrimaryDisplayText();
				const isTimeMode = batteryDisplayMode === 'time';
				const isPowerCapacityMode = batteryDisplayMode === 'power-capacity';
				const secondaryLine = isTimeMode
					? (latestBatteryClockLabel || 'Tom/Fuld kl. -')
					: (isPowerCapacityMode
						? (latestBatteryCapacityKwh !== null ? `Kapacitet ${toDa(latestBatteryCapacityKwh)} kWh` : 'Kapacitet -')
						: latestBatteryFlowText);

				setText('pbBatteryValue', displayValue);
				setText('pbBatteryMeta', secondaryLine);
				setText('pbBatteryBadgeSoc', displayValue);
				setText('pbBatteryBadgeFlow', secondaryLine);
			}

			function toggleBatteryDisplayMode() {
				if (batteryDisplayMode === 'percent') {
					batteryDisplayMode = 'time';
				} else if (batteryDisplayMode === 'time') {
					batteryDisplayMode = 'power-capacity';
				} else {
					batteryDisplayMode = 'percent';
				}
				renderBatteryPrimaryValue();
			}

			function bindBatteryToggle(el) {
				if (!el) return;
				el.style.cursor = 'pointer';
				el.title = 'Skift mellem %, tid og kapacitet';
				el.addEventListener('click', function (event) {
					event.preventDefault();
					event.stopPropagation();
					toggleBatteryDisplayMode();
				});
			}

			const cls = {
				net: ['net-import', 'net-export', 'net-idle'],
				battery: ['battery-charge', 'battery-discharge', 'battery-idle']
			};

			function setClass(el, allClasses, nextClass) {
				if (!el) return;
				allClasses.forEach(c => el.classList.remove(c));
				if (nextClass) el.classList.add(nextClass);
			}

			function setText(id, text) {
				const el = document.getElementById(id);
				if (el) el.textContent = text;
			}

			function asBool(value) {
				if (typeof value === 'boolean') return value;
				if (typeof value === 'number') return value !== 0;
				const text = String(value || '').trim().toLowerCase();
				return text === '1' || text === 'true' || text === 'yes' || text === 'on';
			}

			function setDisplay(id, display) {
				const el = document.getElementById(id);
				if (el) el.style.display = display;
			}

			function updateEvRealtime(data) {
				const evAvailable = data && data.ev_available !== false;
				const evCharging = asBool(data && data.ev_charging);
				const evPowerRaw = Math.max(0, asNum(data && data.ev_power_w));
				const evPowerW = (evPowerRaw > 0 && evPowerRaw < 50) ? (evPowerRaw * 1000) : evPowerRaw;
				const shouldShow = evAvailable && (evCharging || evPowerW >= 150);
				const shouldShowInline = evPowerW > 0;

				if (!shouldShow) {
					setDisplay('pbEvBadge', 'none');
					setDisplay('pbEvInline', 'none');
					return;
				}

				const kwText = `${(evPowerW / 1000).toFixed(1).replace('.', ',')} kW`;

				setText('pbEvBadgeValue', kwText);
				setText('pbEvInlineValue', kwText);
				setDisplay('pbEvBadge', 'inline-flex');
				setDisplay('pbEvInline', shouldShowInline ? 'inline-flex' : 'none');
			}

			function updateHeroRealtime(data) {
				const pvPowerW = asNum(data && data.pv_power_w);
				const batterySoc = asNum(data && data.battery_soc);
				const batteryPowerW = asNum(data && data.battery_power_w);
				const loadPowerW = asNum(data && data.load_power_w);
				const gridPowerW = asNum(data && data.grid_power_w);
				const batteryCapacityKwh = resolveBatteryCapacityKwh(data);
				const reserveMinSoc = resolveReserveMinSoc(data);

				const gridArrow = gridPowerW > 0 ? '↓' : (gridPowerW < 0 ? '↑' : '•');
				const gridLabel = gridPowerW > 0 ? 'Køber' : (gridPowerW < 0 ? 'Sælger' : 'Standby');
				const gridClass = gridPowerW > 0 ? 'net-import' : (gridPowerW < 0 ? 'net-export' : 'net-idle');

				const batteryCharging = batteryPowerW < -10;
				const batteryDischarging = batteryPowerW > 10;
				const battArrow = batteryCharging ? '↑' : (batteryDischarging ? '↓' : '');
				const battLabel = batteryCharging ? 'Lader' : (batteryDischarging ? 'Aflader' : 'Standby');
				const battClass = batteryCharging ? 'battery-charge' : (batteryDischarging ? 'battery-discharge' : 'battery-idle');

				latestBatterySoc = batterySoc;
				latestBatteryPowerW = batteryPowerW;
				latestBatteryCapacityKwh = Number.isFinite(batteryCapacityKwh) && batteryCapacityKwh > 0 ? batteryCapacityKwh : null;
				latestBatteryEtaMinutes = computeEtaMinutesFromPower(batterySoc, batteryPowerW, batteryCapacityKwh, reserveMinSoc);
				latestBatteryFlowText = batteryCharging || batteryDischarging
					? `${battArrow} ${battLabel} ${formatPower(Math.abs(batteryPowerW))}`
					: battLabel;
				latestBatteryClockLabel = '';
				if (latestBatteryEtaMinutes !== null) {
					const clockText = formatClockFromNow(latestBatteryEtaMinutes);
					if (clockText) {
						if (batteryDischarging) {
							latestBatteryClockLabel = `Tom kl. ${clockText}`;
						} else if (batteryCharging) {
							latestBatteryClockLabel = `Fuld kl. ${clockText}`;
						}
					}
				}

				// Når grid er 0 kW kan vi hverken købe eller sælge – skjul pil og tekst
				const gridNetDisplay = gridPowerW === 0 ? '–' : `${gridArrow} ${formatPower(Math.abs(gridPowerW))}`;

				setText('pbNetValue', gridNetDisplay);
				setText('pbNetMeta', gridLabel);
				setText('pbSolarValue', formatPower(Math.max(0, pvPowerW)));
				setText('pbConsumptionValue', formatPower(Math.max(0, loadPowerW)));
				renderBatteryPrimaryValue();

				setText('pbNetBadgeValue', gridNetDisplay);
				setText('pbSolarBadgeValue', formatPower(Math.max(0, pvPowerW)));
				setText('pbConsumptionBadgeValue', formatPower(Math.max(0, loadPowerW)));

				setClass(document.getElementById('pbNetStat'), cls.net, gridClass);
				setClass(document.getElementById('pbNetBadge'), cls.net, gridClass);
				setClass(document.getElementById('pbBatteryStat'), cls.battery, battClass);
				setClass(document.getElementById('pbBatteryBadge'), cls.battery, battClass);
				updateEvRealtime(data);
			}

			bindBatteryToggle(document.getElementById('pbBatteryStat'));
			bindBatteryToggle(document.getElementById('pbBatteryValue'));
			bindBatteryToggle(document.getElementById('pbBatteryMeta'));
			bindBatteryToggle(document.getElementById('pbBatteryBadge'));
			bindBatteryToggle(document.getElementById('pbBatteryBadgeSoc'));
			bindBatteryToggle(document.getElementById('pbBatteryBadgeFlow'));

			async function refreshRealtime() {
				try {
					const res = await fetch('?action=inverter-realtime', { cache: 'no-store' });
					if (!res.ok) return;
					const data = await res.json();
					if (!data || data.ok === false) return;
					updateHeroRealtime(data);
				} catch (_) {
					// Keep existing UI values on transient network errors.
				}
			}

			refreshRealtime();
			setInterval(refreshRealtime, 5000);
		})();

		(function () {
			const batteryStat = document.getElementById('pbBatteryStat');
			if (!batteryStat) return;
			const triggers = [
				document.getElementById('pbBatteryLinkDesktop'),
				document.getElementById('pbBatteryLinkMobile')
			].filter(Boolean);
			if (!triggers.length) return;
			let url = '';

			function applyBatteryLinkTarget(nextLink) {
				const link = String(nextLink || '')
					.replace(/"/g, '')
					.trim();
				url = /^https?:\/\//i.test(link) ? link : '';

				if (!url) {
					triggers.forEach(function (trigger) {
						trigger.style.cursor = 'default';
						trigger.removeAttribute('role');
						trigger.removeAttribute('tabindex');
						trigger.removeAttribute('title');
					});
					return;
				}

				triggers.forEach(function (trigger) {
					trigger.style.cursor = 'pointer';
					trigger.setAttribute('role', 'button');
					trigger.setAttribute('tabindex', '0');
					trigger.setAttribute('title', 'Åbn batterioversigt');
				});
			}

			window.powerBuddyApplyBatteryLinkTarget = function (nextLink) {
				batteryStat.setAttribute('data-battery-link', String(nextLink || ''));
				applyBatteryLinkTarget(nextLink);
			};

			applyBatteryLinkTarget(batteryStat.getAttribute('data-battery-link') || '');

			(async function refreshBatteryLinkFromAuthStatus() {
				try {
					const res = await fetch('?action=planning-auth-status', { cache: 'no-store' });
					if (!res.ok) return;
					const data = await res.json();
					if (!data || !data.authorized) return;
					if (typeof window.powerBuddyApplyBatteryLinkTarget === 'function') {
						window.powerBuddyApplyBatteryLinkTarget(data.battery_link || '');
					}
				} catch (_) {
					// Keep existing target if auth status cannot be fetched.
				}
			})();

			triggers.forEach(function (trigger) {
				trigger.addEventListener('click', function (event) {
					if (!url) return;
					event.preventDefault();
					event.stopPropagation();
					window.open(url, '_blank', 'noopener,noreferrer');
				});
				trigger.addEventListener('keydown', function (event) {
					if (!url) return;
					if (event.key === 'Enter' || event.key === ' ') {
						event.preventDefault();
						event.stopPropagation();
						window.open(url, '_blank', 'noopener,noreferrer');
					}
				});
			});
		})();
