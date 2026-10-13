// FinX Frontend Controller - Real-Time Streaming & Visualizer

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
    throughput: 0.0
  };

  // DOM Elements
  const valTotalEvents = document.getElementById('val-total-events');
  const valNormalEvents = document.getElementById('val-normal-events');
  const valAnomalies = document.getElementById('val-anomalies-detected');
  const valThroughput = document.getElementById('val-throughput');
  const feedContainer = document.getElementById('feed-container');
  const feedEmptyState = document.getElementById('feed-empty-state');
  const feedVisibleCount = document.getElementById('feed-visible-count');
  const engineStatusText = document.getElementById('engine-status-text');
  const kafkaIndicator = document.getElementById('kafka-indicator');
  const payloadJsonCode = document.getElementById('payload-json-code');
  const payloadSourceTag = document.getElementById('payload-source-tag');

  // Controls
  const btnReplayControlled = document.getElementById('btn-replay-controlled');
  const btnReplayHuge = document.getElementById('btn-replay-huge');
  const btnStop = document.getElementById('btn-stop-replay');
  const btnClear = document.getElementById('btn-clear-feed');

  // Anomaly Banner Elements
  const anomalyBanner = document.getElementById('anomaly-alert-banner');
  const anomalyTxnId = document.getElementById('anomaly-txn-id');
  const anomalyAmount = document.getElementById('anomaly-amount');
  const anomalyMerchant = document.getElementById('anomaly-merchant');
  const btnDismissBanner = document.getElementById('btn-dismiss-banner');

  // Canvas
  const canvas = document.getElementById('amount-chart');
  const ctx = canvas.getContext('2d');

  // Speed Buttons
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
      renderFeed();
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
    renderFeed();
    drawChart();
    payloadJsonCode.textContent = '// Feed cleared';
  });

  // Replay Triggers
  btnReplayControlled.addEventListener('click', () => triggerReplay('controlled'));
  btnReplayHuge.addEventListener('click', () => triggerReplay('huge'));
  btnStop.addEventListener('click', stopReplay);

  function triggerReplay(dataset) {
    setPlaying(true);
    fetch(`/api/replay?dataset=${dataset}&speed=${state.selectedSpeed}`, { method: 'POST' })
      .then(res => res.json())
      .catch(err => console.error('Error triggering replay:', err));
  }

  function stopReplay() {
    fetch('/api/stop', { method: 'POST' })
      .then(res => res.json())
      .then(() => setPlaying(false))
      .catch(err => console.error('Error stopping replay:', err));
  }

  function setPlaying(active) {
    state.isPlaying = active;
    btnStop.disabled = !active;
    btnReplayControlled.disabled = active;
    btnReplayHuge.disabled = active;
    engineStatusText.textContent = active ? 'Streaming...' : 'Standby';
    engineStatusText.style.color = active ? '#06b6d4' : '#f8fafc';
  }

  // Format Helpers
  function formatMoney(amount) {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount);
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

  // Process Incoming Kafka Event
  function handleEvent(data) {
    if (data.type === 'replay_started') {
      setPlaying(true);
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

    // Check if valid transaction payload
    if (!data.transaction_id || data.amount === undefined) return;

    // Detect Anomaly
    const isAnomaly = data.is_anomaly || data.amount > 5000.0 || (data.merchant && data.merchant.includes('Unknown'));
    data.is_anomaly = isAnomaly;

    // Update Counters
    state.totalEvents++;
    if (isAnomaly) {
      state.anomalies++;
      triggerAnomalyAlert(data);
    } else {
      state.normalEvents++;
    }

    // Throughput tracking
    state.recentEventCount++;
    const now = Date.now();
    if (now - state.lastTimeWindow >= 1000) {
      state.throughput = (state.recentEventCount / ((now - state.lastTimeWindow) / 1000)).toFixed(1);
      state.recentEventCount = 0;
      state.lastTimeWindow = now;
      valThroughput.innerHTML = `${state.throughput} <span class="metric-unit">msg/s</span>`;
    }

    // Store in State
    state.events.unshift(data);
    if (state.events.length > 500) state.events.pop();

    // Chart Point
    state.chartPoints.push({
      time: now,
      amount: data.amount,
      isAnomaly: isAnomaly
    });
    if (state.chartPoints.length > 60) state.chartPoints.shift();

    updateMetrics();
    renderFeed();
    drawChart();

    // Select payload if first or anomaly
    if (isAnomaly || state.events.length === 1) {
      selectPayload(data);
    }
  }

  function triggerAnomalyAlert(data) {
    anomalyTxnId.textContent = data.transaction_id.substring(0, 13) + '...';
    anomalyAmount.textContent = formatMoney(data.amount);
    anomalyMerchant.textContent = data.merchant;
    anomalyBanner.classList.remove('hidden');

    // Auto dismiss after 12s
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
    
    // Clean display payload (without internal server tags)
    const clean = {
      transaction_id: data.transaction_id,
      account_id: data.account_id,
      timestamp: data.timestamp,
      amount: data.amount,
      merchant: data.merchant,
      category: data.category,
      channel: data.channel
    };
    if (data.is_anomaly) {
      clean.is_anomaly_detected = true;
      clean.anomaly_rationale = "Blueprint Seeded Scenario: Outlier Amount > $5,000 threshold";
    }

    payloadJsonCode.textContent = JSON.stringify(clean, null, 2);

    // Highlight selected card
    document.querySelectorAll('.txn-card').forEach(c => {
      c.classList.toggle('selected', c.dataset.id === data.transaction_id);
    });
  }

  function renderFeed() {
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

    // Build cards
    const fragment = document.createDocumentFragment();
    filtered.slice(0, 100).forEach((txn, idx) => {
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
          <div class="txn-amount ${txn.is_anomaly ? 'amount-anomaly' : 'amount-normal'}">
            ${formatMoney(txn.amount)}
          </div>
        </div>
        <div class="txn-card-body">
          <div class="txn-merchant">${txn.merchant}</div>
          <div class="txn-category-tag">${txn.category}</div>
        </div>
        <div class="txn-card-footer">
          <span>${formatTime(txn.timestamp)}</span>
          ${txn.is_anomaly ? '<span class="anomaly-pill">⚠️ Seeded Anomaly</span>' : '<span style="color: #64748b">Verified ✓</span>'}
        </div>
      `;

      card.addEventListener('click', () => selectPayload(txn));
      fragment.appendChild(card);
    });

    feedContainer.innerHTML = '';
    feedContainer.appendChild(fragment);
  }

  // Draw Dynamic Real-Time Chart
  function drawChart() {
    const w = canvas.width = canvas.offsetWidth * window.devicePixelRatio;
    const h = canvas.height = canvas.offsetHeight * window.devicePixelRatio;
    ctx.clearRect(0, 0, w, h);

    if (state.chartPoints.length < 2) {
      // Draw placeholder grid
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0, h * 0.75);
      ctx.lineTo(w, h * 0.75);
      ctx.stroke();
      return;
    }

    const maxAmount = Math.max(200, ...state.chartPoints.map(p => p.amount));
    const padX = 20;
    const padY = 25;
    const chartW = w - padX * 2;
    const chartH = h - padY * 2;

    // Draw baseline grid lines
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.06)';
    ctx.lineWidth = 1;
    for (let r = 0; r <= 3; r++) {
      const y = h - padY - (chartH * (r / 3));
      ctx.beginPath();
      ctx.moveTo(padX, y);
      ctx.lineTo(w - padX, y);
      ctx.stroke();
    }

    // Draw bars / line
    const step = chartW / (state.chartPoints.length - 1 || 1);

    // Baseline area fill
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

    // Plot points
    state.chartPoints.forEach((p, i) => {
      const x = padX + i * step;
      const y = h - padY - ((p.amount / maxAmount) * chartH);

      if (p.isAnomaly) {
        // Red glowing anomaly spike
        ctx.shadowColor = 'rgba(239, 68, 68, 0.9)';
        ctx.shadowBlur = 15;
        ctx.fillStyle = '#ef4444';
        ctx.beginPath();
        ctx.arc(x, y, 6 * window.devicePixelRatio, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;

        // Label anomaly value
        ctx.fillStyle = '#fca5a5';
        ctx.font = `bold ${10 * window.devicePixelRatio}px JetBrains Mono`;
        ctx.fillText(`$${p.amount.toFixed(0)}`, x - 20, Math.max(20, y - 10));
      } else {
        // Cyan normal point
        ctx.fillStyle = '#06b6d4';
        ctx.beginPath();
        ctx.arc(x, y, 2.5 * window.devicePixelRatio, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  }

  // Connect SSE Stream
  function connectStream() {
    console.log('[FinX] Establishing SSE stream connection...');
    const eventSource = new EventSource('/api/events');

    eventSource.onopen = () => {
      kafkaIndicator.className = 'status-dot dot-online';
      console.log('[FinX] SSE connected.');
    };

    eventSource.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data);
        handleEvent(payload);
      } catch (err) {
        console.error('Error parsing SSE data:', err);
      }
    };

    eventSource.onerror = (err) => {
      kafkaIndicator.className = 'status-dot dot-pulse';
      console.warn('[FinX] SSE connection interrupted, retrying...', err);
    };
  }

  // Poll Server Status
  function checkStatus() {
    fetch('/api/status')
      .then(res => res.json())
      .then(data => {
        if (data.kafka_connected) {
          kafkaIndicator.className = 'status-dot dot-online';
        } else {
          kafkaIndicator.className = 'status-dot dot-pulse';
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

  // Load Initial History
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

  // Initialize
  connectStream();
  loadHistory();
  setInterval(checkStatus, 3000);
  window.addEventListener('resize', drawChart);
});
