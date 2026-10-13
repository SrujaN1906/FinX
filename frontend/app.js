// FinX Frontend Controller - Real-Time Multi-Currency Streaming & Anomaly Visualizer

document.addEventListener('DOMContentLoaded', () => {
  // State
  const state = {
    totalEvents: 0,
    normalEvents: 0,
    anomalies: 0,
    events: [],
    filter: 'all',
    selectedSpeed: 1.0,
    isPlaying: false,
    chartPoints: [],
    lastTimeWindow: Date.now(),
    recentEventCount: 0,
    throughput: 0.0,
    currentFileName: 'transactions_controlled.csv',
    renderTimer: null,
    pendingEvents: [],
    exchangeRates: {
      USD: 86.50,
      EUR: 93.80,
      GBP: 110.20,
      AED: 23.55,
      SGD: 64.70,
      CAD: 61.20
    }
  };

  // DOM Elements
  const valTotalEvents = document.getElementById('val-total-events');
  const valNormalEvents = document.getElementById('val-normal-events');
  const valAnomalies = document.getElementById('val-anomalies-detected');
  const valThroughput = document.getElementById('val-throughput');
  const valFileNameFooter = document.getElementById('val-file-name-footer');
  const feedContainer = document.getElementById('feed-container');
  const feedEmptyState = document.getElementById('feed-empty-state');
  const feedVisibleCount = document.getElementById('feed-visible-count');
  const engineStatusText = document.getElementById('engine-status-text');
  const kafkaIndicator = document.getElementById('kafka-indicator');
  const payloadJsonCode = document.getElementById('payload-json-code');
  const payloadSourceTag = document.getElementById('payload-source-tag');

  // Controls
  const fileInput = document.getElementById('csv-file-input');
  const btnUpload = document.getElementById('btn-upload-file');
  const selectedFileBadge = document.getElementById('selected-file-badge');
  const btnStreamDefault = document.getElementById('btn-stream-default');
  const btnStop = document.getElementById('btn-stop-replay');
  const btnClear = document.getElementById('btn-clear-feed');

  // Anomaly Banner Elements
  const anomalyBanner = document.getElementById('anomaly-alert-banner');
  const anomalyTxnId = document.getElementById('anomaly-txn-id');
  const anomalyInr = document.getElementById('anomaly-inr');
  const anomalyOrig = document.getElementById('anomaly-orig');
  const anomalyMerchant = document.getElementById('anomaly-merchant');
  const anomalyRationaleText = document.getElementById('anomaly-rationale-text');
  const btnDismissBanner = document.getElementById('btn-dismiss-banner');

  // Canvas
  const canvas = document.getElementById('amount-chart');
  const ctx = canvas.getContext('2d');

  // Speed Selector Buttons (Only Realtime 1.0x and 2.0x)
  const speedButtons = document.querySelectorAll('.speed-btn');
  speedButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      speedButtons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.selectedSpeed = parseFloat(btn.dataset.speed);
    });
  });

  // Filter Tabs
  const filterTabs = document.querySelectorAll('.filter-tab');
  filterTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      filterTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      state.filter = tab.dataset.filter;
      renderFeedImmediate();
    });
  });

  // Dismiss Banner
  btnDismissBanner.addEventListener('click', () => {
    anomalyBanner.classList.add('hidden');
  });

  // Clear Feed
  btnClear.addEventListener('click', () => {
    state.events = [];
    state.chartPoints = [];
    state.totalEvents = 0;
    state.normalEvents = 0;
    state.anomalies = 0;
    updateMetrics();
    renderFeedImmediate();
    drawChart();
    payloadJsonCode.textContent = '// Feed cleared';
  });

  // File Upload Trigger
  btnUpload.addEventListener('click', () => {
    fileInput.click();
  });

  fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (!file) return;

    selectedFileBadge.textContent = file.name;
    state.currentFileName = file.name;
    valFileNameFooter.textContent = `File: ${file.name}`;

    uploadAndStream(file);
  });

  // Sample Stream Trigger
  btnStreamDefault.addEventListener('click', () => {
    setPlaying(true);
    selectedFileBadge.textContent = 'transactions_controlled.csv';
    state.currentFileName = 'transactions_controlled.csv';
    valFileNameFooter.textContent = 'File: transactions_controlled.csv';

    fetch(`/api/replay_default?speed=${state.selectedSpeed}`, { method: 'POST' })
      .then(res => res.json())
      .catch(err => console.error('Error starting default stream:', err));
  });

  // Stop Stream Trigger
  btnStop.addEventListener('click', stopReplay);

  function uploadAndStream(file) {
    setPlaying(true);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('speed', state.selectedSpeed.toString());

    fetch('/api/upload', {
      method: 'POST',
      body: formData
    })
    .then(res => res.json())
    .catch(err => {
      console.error('Upload failed:', err);
      setPlaying(false);
    });
  }

  function stopReplay() {
    fetch('/api/stop', { method: 'POST' })
      .then(res => res.json())
      .then(() => setPlaying(false))
      .catch(err => console.error('Error stopping stream:', err));
  }

  function setPlaying(active) {
    state.isPlaying = active;
    btnStop.disabled = !active;
    btnUpload.disabled = active;
    btnStreamDefault.disabled = active;
    engineStatusText.textContent = active ? 'Streaming...' : 'Standby';
    engineStatusText.style.color = active ? '#06b6d4' : '#f8fafc';
  }

  // Currency Formatters
  function formatINR(amount) {
    return new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 2 }).format(amount);
  }

  function formatOriginal(amount, currency) {
    const curr = (currency || 'USD').toUpperCase();
    try {
      return new Intl.NumberFormat('en-US', { style: 'currency', currency: curr, maximumFractionDigits: 2 }).format(amount);
    } catch {
      return `${curr} ${Number(amount).toFixed(2)}`;
    }
  }

  function formatTime(isoStr) {
    if (!isoStr) return '';
    try {
      const d = new Date(isoStr);
      return d.toLocaleTimeString();
    } catch {
      return isoStr;
    }
  }

  // Handle Event Batching for Lag-Free Ingestion of Innumerable Transactions
  function handleEvent(data) {
    if (data.type === 'replay_started') {
      setPlaying(true);
      if (data.filename) {
        selectedFileBadge.textContent = data.filename;
        valFileNameFooter.textContent = `File: ${data.filename} (${data.total_rows} rows)`;
      }
      return;
    }
    if (data.type === 'replay_finished') {
      setPlaying(false);
      return;
    }
    if (data.type === 'connected') {
      kafkaIndicator.className = 'status-dot dot-online';
      return;
    }
    if (data.type === 'system_log') {
      console.log('System log:', data.message);
      return;
    }

    if (!data.transaction_id || data.amount === undefined) return;

    // Normalize INR amount if server hasn't already done so
    if (data.amount_inr === undefined) {
      const rate = state.exchangeRates[data.currency] || 86.50;
      data.amount_inr = Math.round(data.amount * rate * 100) / 100;
      data.conversion_rate = rate;
    }

    // Comprehensive Anomaly Detection Check in INR
    const isAnomaly = data.is_anomaly || data.amount_inr >= 50000.0 || (data.merchant && data.merchant.toLowerCase().includes('unknown'));
    data.is_anomaly = isAnomaly;

    state.totalEvents++;
    if (isAnomaly) {
      state.anomalies++;
      triggerAnomalyAlert(data);
    } else {
      state.normalEvents++;
    }

    // Throughput calculation
    state.recentEventCount++;
    const now = Date.now();
    if (now - state.lastTimeWindow >= 1000) {
      state.throughput = (state.recentEventCount / ((now - state.lastTimeWindow) / 1000)).toFixed(1);
      state.recentEventCount = 0;
      state.lastTimeWindow = now;
      valThroughput.innerHTML = `${state.throughput} <span class="metric-unit">msg/s</span>`;
    }

    // Keep events in memory (capped to 500 for extreme rendering performance)
    state.events.unshift(data);
    if (state.events.length > 500) {
      state.events.pop();
    }

    // Chart points in INR
    state.chartPoints.push({
      time: now,
      amount: data.amount_inr,
      isAnomaly: isAnomaly
    });
    if (state.chartPoints.length > 60) {
      state.chartPoints.shift();
    }

    // Auto-select first transaction or anomaly
    if (isAnomaly || state.events.length === 1) {
      selectPayload(data);
    }

    // Schedule debounced render to prevent browser lag under heavy streams
    scheduleBatchRender();
  }

  function scheduleBatchRender() {
    if (!state.renderTimer) {
      state.renderTimer = requestAnimationFrame(() => {
        updateMetrics();
        renderFeedImmediate();
        drawChart();
        state.renderTimer = null;
      });
    }
  }

  function triggerAnomalyAlert(data) {
    anomalyTxnId.textContent = data.transaction_id.substring(0, 13) + '...';
    anomalyInr.textContent = formatINR(data.amount_inr);
    anomalyOrig.textContent = `(Original: ${formatOriginal(data.amount, data.currency)})`;
    anomalyMerchant.textContent = data.merchant;
    if (data.anomaly_rationale) {
      anomalyRationaleText.textContent = data.anomaly_rationale;
    }
    anomalyBanner.classList.remove('hidden');

    setTimeout(() => {
      anomalyBanner.classList.add('hidden');
    }, 12000);
  }

  function updateMetrics() {
    valTotalEvents.textContent = state.totalEvents.toLocaleString();
    valNormalEvents.textContent = state.normalEvents.toLocaleString();
    valAnomalies.textContent = state.anomalies.toLocaleString();
  }

  function selectPayload(data) {
    payloadSourceTag.textContent = `Kafka Offset: ${data._kafka_offset !== undefined ? data._kafka_offset : 'Live Stream'}`;

    const clean = {
      transaction_id: data.transaction_id,
      account_id: data.account_id,
      timestamp: data.timestamp,
      original_amount: data.amount,
      original_currency: data.currency || 'USD',
      amount_inr: data.amount_inr,
      conversion_rate_applied: `1 ${data.currency || 'USD'} = ₹${data.conversion_rate || 86.50}`,
      merchant: data.merchant,
      category: data.category,
      channel: data.channel,
      anomaly_flagged: data.is_anomaly
    };
    if (data.is_anomaly) {
      clean.anomaly_rationale = data.anomaly_rationale || "Normalized value exceeds ₹50,000 threshold or flagged merchant";
    }

    payloadJsonCode.textContent = JSON.stringify(clean, null, 2);

    document.querySelectorAll('.txn-card').forEach(c => {
      c.classList.toggle('selected', c.dataset.id === data.transaction_id);
    });
  }

  function renderFeedImmediate() {
    const filtered = state.events.filter(e => {
      if (state.filter === 'anomalies') return e.is_anomaly;
      if (state.filter === 'normal') return !e.is_anomaly;
      return true;
    });

    feedVisibleCount.textContent = `${filtered.length} visible`;

    if (filtered.length === 0) {
      feedEmptyState.style.display = 'flex';
      feedContainer.innerHTML = '';
      feedContainer.appendChild(feedEmptyState);
      return;
    }

    feedEmptyState.style.display = 'none';

    // Efficiently render max 60 visible cards in viewport
    const fragment = document.createDocumentFragment();
    filtered.slice(0, 60).forEach((txn, idx) => {
      const card = document.createElement('div');
      card.className = `txn-card ${txn.is_anomaly ? 'anomaly-card' : ''}`;
      card.dataset.id = txn.transaction_id;

      card.innerHTML = `
        <div class="txn-card-header">
          <div class="txn-left-meta">
            <span class="txn-seq">#${txn.index || (state.totalEvents - idx)}</span>
            <span class="account-badge">${txn.account_id}</span>
            <span class="txn-channel">${txn.channel}</span>
          </div>
          <div class="txn-amounts-dual">
            <div class="amount-inr ${txn.is_anomaly ? 'amount-anomaly' : ''}">
              ${formatINR(txn.amount_inr)}
            </div>
            <div class="amount-orig">
              ${formatOriginal(txn.amount, txn.currency)}
            </div>
          </div>
        </div>
        <div class="txn-card-body">
          <div class="txn-merchant">${txn.merchant}</div>
          <div class="txn-category-tag">${txn.category}</div>
        </div>
        <div class="txn-card-footer">
          <span>${formatTime(txn.timestamp)}</span>
          ${txn.is_anomaly ? '<span class="anomaly-pill">⚠️ Anomaly Detected</span>' : '<span style="color: #64748b">Verified ✓</span>'}
        </div>
      `;

      card.addEventListener('click', () => selectPayload(txn));
      fragment.appendChild(card);
    });

    feedContainer.innerHTML = '';
    feedContainer.appendChild(fragment);
  }

  // Draw Dynamic Real-Time Chart in INR
  function drawChart() {
    const w = canvas.width = canvas.offsetWidth * window.devicePixelRatio;
    const h = canvas.height = canvas.offsetHeight * window.devicePixelRatio;
    ctx.clearRect(0, 0, w, h);

    if (state.chartPoints.length < 2) {
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0, h * 0.75);
      ctx.lineTo(w, h * 0.75);
      ctx.stroke();
      return;
    }

    const maxAmount = Math.max(10000, ...state.chartPoints.map(p => p.amount));
    const padX = 20;
    const padY = 25;
    const chartW = w - padX * 2;
    const chartH = h - padY * 2;

    // Grid Lines
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.06)';
    ctx.lineWidth = 1;
    for (let r = 0; r <= 3; r++) {
      const y = h - padY - (chartH * (r / 3));
      ctx.beginPath();
      ctx.moveTo(padX, y);
      ctx.lineTo(w - padX, y);
      ctx.stroke();
    }

    const step = chartW / (state.chartPoints.length - 1 || 1);

    // Area Fill
    ctx.beginPath();
    state.chartPoints.forEach((p, i) => {
      const x = padX + i * step;
      const y = h - padY - ((p.amount / maxAmount) * chartH);
      if (i === 0) ctx.moveTo(x, h - padY);
      ctx.lineTo(x, y);
    });
    ctx.lineTo(padX + (state.chartPoints.length - 1) * step, h - padY);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, 'rgba(6, 182, 212, 0.25)');
    grad.addColorStop(1, 'rgba(6, 182, 212, 0.0)');
    ctx.fillStyle = grad;
    ctx.fill();

    // Line & Points
    state.chartPoints.forEach((p, i) => {
      const x = padX + i * step;
      const y = h - padY - ((p.amount / maxAmount) * chartH);

      if (p.isAnomaly) {
        ctx.shadowColor = 'rgba(239, 68, 68, 0.9)';
        ctx.shadowBlur = 15;
        ctx.fillStyle = '#ef4444';
        ctx.beginPath();
        ctx.arc(x, y, 6 * window.devicePixelRatio, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;

        ctx.fillStyle = '#fca5a5';
        ctx.font = `bold ${10 * window.devicePixelRatio}px JetBrains Mono`;
        ctx.fillText(`₹${Math.round(p.amount).toLocaleString('en-IN')}`, x - 25, Math.max(20, y - 10));
      } else {
        ctx.fillStyle = '#06b6d4';
        ctx.beginPath();
        ctx.arc(x, y, 2.5 * window.devicePixelRatio, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  }

  // Connect SSE Stream
  function connectStream() {
    const eventSource = new EventSource('/api/events');

    eventSource.onopen = () => {
      kafkaIndicator.className = 'status-dot dot-online';
      console.log('[FinX] Connected to live event stream.');
    };

    eventSource.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data);
        handleEvent(payload);
      } catch (err) {
        console.error('Error parsing SSE payload:', err);
      }
    };

    eventSource.onerror = () => {
      kafkaIndicator.className = 'status-dot dot-pulse';
    };
  }

  // Load Status
  function checkStatus() {
    fetch('/api/status')
      .then(res => res.json())
      .then(data => {
        if (data.kafka_connected) {
          kafkaIndicator.className = 'status-dot dot-online';
        } else {
          kafkaIndicator.className = 'status-dot dot-pulse';
        }
        if (data.rates) {
          state.exchangeRates = data.rates;
        }
        if (data.active_replay && !state.isPlaying) {
          setPlaying(true);
        } else if (!data.active_replay && state.isPlaying) {
          setPlaying(false);
        }
      })
      .catch(() => {
        kafkaIndicator.className = 'status-dot';
      });
  }

  function loadHistory() {
    fetch('/api/history')
      .then(res => res.json())
      .then(data => {
        if (data.events && data.events.length > 0) {
          data.events.forEach(ev => {
            if (ev.transaction_id && ev.amount !== undefined) {
              handleEvent(ev);
            }
          });
        }
      })
      .catch(err => console.error('Failed to load history:', err));
  }

  // Init
  connectStream();
  loadHistory();
  setInterval(checkStatus, 3000);
  window.addEventListener('resize', drawChart);
});
