package main

import (
	"html/template"
	"net/http"

	"github.com/labstack/echo/v4"
)

var indexTmpl = template.Must(template.New("index").Parse(indexHTML))

func (d *Dashboard) indexPage(c echo.Context) error {
	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response().WriteHeader(http.StatusOK)
	return indexTmpl.Execute(c.Response(), nil)
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SagaForge AI — Dashboard</title>
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <script src="https://unpkg.com/htmx-ext-sse@2.2.2/sse.js"></script>
  <style>
    :root {
      --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
      --text: #e4e4e7; --muted: #71717a; --accent: #6366f1;
      --green: #22c55e; --red: #ef4444; --yellow: #eab308; --blue: #3b82f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Inter', -apple-system, system-ui, sans-serif; background: var(--bg); color: var(--text); }

    .header {
      padding: 20px 32px; border-bottom: 1px solid var(--border);
      display: flex; justify-content: space-between; align-items: center;
    }
    .header h1 { font-size: 1.5rem; font-weight: 700; }
    .header h1 span { color: var(--accent); }
    .header .tagline { color: var(--muted); font-size: 0.85rem; }

    .container { max-width: 1400px; margin: 0 auto; padding: 24px 32px; }

    /* Stats bar */
    .stats { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 24px; }
    .stat-card {
      background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
      padding: 20px; text-align: center;
    }
    .stat-card .label { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
    .stat-card .value { font-size: 2rem; font-weight: 700; margin-top: 4px; }
    .stat-card.total .value { color: var(--text); }
    .stat-card.completed .value { color: var(--green); }
    .stat-card.failed .value { color: var(--red); }
    .stat-card.compensated .value { color: var(--yellow); }
    .stat-card.active .value { color: var(--blue); }

    /* Actions */
    .actions { display: flex; gap: 12px; margin-bottom: 24px; }
    .btn {
      padding: 10px 20px; border-radius: 8px; border: none; cursor: pointer;
      font-size: 0.9rem; font-weight: 600; transition: all 0.15s;
    }
    .btn-primary { background: var(--accent); color: white; }
    .btn-primary:hover { background: #4f46e5; }
    .btn-outline { background: transparent; color: var(--text); border: 1px solid var(--border); }
    .btn-outline:hover { border-color: var(--accent); }

    /* Main layout */
    .main { display: grid; grid-template-columns: 1fr 400px; gap: 24px; }

    /* Saga table */
    .panel {
      background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
      overflow: hidden;
    }
    .panel-header { padding: 16px 20px; border-bottom: 1px solid var(--border); font-weight: 600; font-size: 0.95rem; }
    table { width: 100%; border-collapse: collapse; }
    th { text-align: left; padding: 12px 16px; font-size: 0.75rem; text-transform: uppercase; color: var(--muted); border-bottom: 1px solid var(--border); }
    td { padding: 12px 16px; border-bottom: 1px solid var(--border); font-size: 0.875rem; }
    tr:hover { background: rgba(99,102,241,0.05); }

    .badge {
      display: inline-block; padding: 2px 10px; border-radius: 999px;
      font-size: 0.75rem; font-weight: 600;
    }
    .badge-completed { background: rgba(34,197,94,0.15); color: var(--green); }
    .badge-failed { background: rgba(239,68,68,0.15); color: var(--red); }
    .badge-compensated { background: rgba(234,179,8,0.15); color: var(--yellow); }
    .badge-in_progress, .badge-started { background: rgba(59,130,246,0.15); color: var(--blue); }
    .badge-compensating { background: rgba(234,179,8,0.15); color: var(--yellow); }

    /* Live event feed */
    .feed { max-height: 600px; overflow-y: auto; }
    .feed-item {
      padding: 12px 16px; border-bottom: 1px solid var(--border);
      font-size: 0.8rem; font-family: 'JetBrains Mono', monospace;
      animation: fadeIn 0.3s;
    }
    .feed-item .event-type { color: var(--accent); font-weight: 600; }
    .feed-item .timestamp { color: var(--muted); }

    /* Detail panel (shown on row click) */
    .detail-panel {
      background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
      padding: 20px; margin-top: 24px;
    }
    .detail-panel h3 { margin-bottom: 12px; }
    .insight-card {
      background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
      padding: 16px; margin-top: 12px;
    }
    .risk-high { border-left: 3px solid var(--red); }
    .risk-medium { border-left: 3px solid var(--yellow); }
    .risk-low { border-left: 3px solid var(--green); }
    .risk-score { font-size: 1.5rem; font-weight: 700; }

    @keyframes fadeIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: none; } }

    /* Loading indicator */
    .htmx-indicator { display: none; }
    .htmx-request .htmx-indicator { display: inline; }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1>Saga<span>Forge</span> AI</h1>
      <div class="tagline">Distributed Transaction Orchestrator — Real-Time Dashboard</div>
    </div>
    <div style="color:var(--muted); font-size:0.8rem;">
      <span id="connection-status" style="color:var(--green);">● Connected</span>
    </div>
  </div>

  <div class="container">
    <!-- Stats -->
    <div class="stats" id="stats-bar" hx-get="/api/stats" hx-trigger="load, every 3s" hx-swap="innerHTML">
      <div class="stat-card total"><div class="label">Total Sagas</div><div class="value">—</div></div>
      <div class="stat-card completed"><div class="label">Completed</div><div class="value">—</div></div>
      <div class="stat-card failed"><div class="label">Failed</div><div class="value">—</div></div>
      <div class="stat-card compensated"><div class="label">Compensated</div><div class="value">—</div></div>
      <div class="stat-card active"><div class="label">In Progress</div><div class="value">—</div></div>
    </div>

    <!-- Actions -->
    <div class="actions">
      <button class="btn btn-primary" hx-post="/api/simulate" hx-swap="none"
              onclick="this.textContent='Creating…'; setTimeout(()=>this.textContent='🚀 Simulate Order', 1500)">
        🚀 Simulate Order
      </button>
      <button class="btn btn-outline" onclick="document.getElementById('saga-table').dispatchEvent(new Event('refresh'))">
        🔄 Refresh
      </button>
    </div>

    <!-- Main grid -->
    <div class="main">
      <!-- Sagas table -->
      <div class="panel">
        <div class="panel-header">Sagas</div>
        <div id="saga-table" hx-get="/api/sagas" hx-trigger="load, refresh, every 5s" hx-swap="innerHTML">
          <table><tbody><tr><td colspan="5" style="text-align:center;padding:40px;color:var(--muted);">Loading sagas…</td></tr></tbody></table>
        </div>
      </div>

      <!-- Live event feed (SSE) -->
      <div class="panel">
        <div class="panel-header">Live Event Feed</div>
        <div class="feed" id="live-feed">
          <div class="feed-item" style="color:var(--muted);">Waiting for events…</div>
        </div>
      </div>
    </div>

    <!-- Detail panel (populated on click) -->
    <div id="detail-area"></div>
  </div>

  <script>
    // ── SSE live feed ──
    const feed = document.getElementById('live-feed');
    const es = new EventSource('/api/stream');
    es.onmessage = (e) => {
      try {
        const evt = JSON.parse(e.data);
        const div = document.createElement('div');
        div.className = 'feed-item';
        const ts = new Date().toLocaleTimeString();
        div.innerHTML = '<span class="timestamp">' + ts + '</span> '
          + '<span class="event-type">' + (evt.type || 'event') + '</span> '
          + '<span style="color:var(--muted)">order:' + (evt.order_id || '').substring(0,8) + '</span>';
        feed.prepend(div);
        // Trim old events
        while (feed.children.length > 100) feed.removeChild(feed.lastChild);
      } catch(err) { /* skip non-JSON */ }
    };
    es.onerror = () => {
      document.getElementById('connection-status').style.color = '#ef4444';
      document.getElementById('connection-status').textContent = '● Disconnected';
    };

    // ── Render stats (HTMX afterSwap) ──
    document.getElementById('stats-bar').addEventListener('htmx:afterSwap', function(e) {
      // The response is JSON but we need to render it as HTML cards
    });
    // Override HTMX swap for stats
    htmx.on('#stats-bar', 'htmx:beforeSwap', function(e) {
      try {
        const d = JSON.parse(e.detail.xhr.responseText);
        e.detail.serverResponse =
          '<div class="stat-card total"><div class="label">Total Sagas</div><div class="value">' + d.total + '</div></div>' +
          '<div class="stat-card completed"><div class="label">Completed</div><div class="value">' + d.completed + '</div></div>' +
          '<div class="stat-card failed"><div class="label">Failed</div><div class="value">' + d.failed + '</div></div>' +
          '<div class="stat-card compensated"><div class="label">Compensated</div><div class="value">' + d.compensated + '</div></div>' +
          '<div class="stat-card active"><div class="label">In Progress</div><div class="value">' + d.in_progress + '</div></div>';
      } catch(err) {}
    });

    // ── Render sagas table ──
    htmx.on('#saga-table', 'htmx:beforeSwap', function(e) {
      try {
        const sagas = JSON.parse(e.detail.xhr.responseText);
        if (!sagas || sagas.length === 0) {
          e.detail.serverResponse = '<table><tbody><tr><td colspan="5" style="text-align:center;padding:40px;color:var(--muted);">No sagas yet. Hit "Simulate Order" to start!</td></tr></tbody></table>';
          return;
        }
        let rows = '<table><thead><tr><th>Order</th><th>Status</th><th>Step</th><th>Customer</th><th>Total</th></tr></thead><tbody>';
        sagas.forEach(s => {
          const status = s.status || 'unknown';
          rows += '<tr style="cursor:pointer" onclick="loadDetail(\'' + s.order_id + '\',\'' + s.id + '\')">'
            + '<td style="font-family:monospace;font-size:0.8rem;">' + (s.order_id||'').substring(0,8) + '…</td>'
            + '<td><span class="badge badge-' + status + '">' + status + '</span></td>'
            + '<td>' + (s.current_step||'—') + '</td>'
            + '<td>' + (s.customer_id||'—') + '</td>'
            + '<td>$' + (s.total ? s.total.toFixed(2) : '—') + '</td>'
            + '</tr>';
        });
        rows += '</tbody></table>';
        e.detail.serverResponse = rows;
      } catch(err) {}
    });

    // ── Load saga detail + AI insights ──
    function loadDetail(orderID, sagaID) {
      const area = document.getElementById('detail-area');
      area.innerHTML = '<div class="detail-panel"><h3>Loading…</h3></div>';

      Promise.all([
        fetch('/api/events/' + orderID).then(r => r.json()),
        fetch('/api/insights/' + orderID).then(r => r.json()).catch(() => []),
      ]).then(([events, insights]) => {
        let html = '<div class="detail-panel">';
        html += '<h3>Order ' + orderID.substring(0,8) + '… — Event Timeline</h3>';
        html += '<div style="margin-top:12px;">';
        (events||[]).forEach(ev => {
          html += '<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:0.85rem;">'
            + '<strong>' + ev.event_type + '</strong> '
            + '<span style="color:var(--muted);font-size:0.75rem;">' + new Date(ev.created_at).toLocaleString() + '</span>'
            + '</div>';
        });
        html += '</div>';

        if (insights && insights.length > 0) {
          html += '<h3 style="margin-top:20px;">🤖 AI Insights</h3>';
          insights.forEach(ins => {
            const risk = ins.risk_score;
            const cls = risk > 0.7 ? 'risk-high' : risk > 0.3 ? 'risk-medium' : 'risk-low';
            const color = risk > 0.7 ? 'var(--red)' : risk > 0.3 ? 'var(--yellow)' : 'var(--green)';
            html += '<div class="insight-card ' + cls + '">'
              + '<div style="display:flex;justify-content:space-between;align-items:center;">'
              + '<span class="badge badge-' + (risk > 0.7 ? 'failed' : risk > 0.3 ? 'compensated' : 'completed') + '">' + ins.trigger_event + '</span>'
              + '<span class="risk-score" style="color:' + color + ';">' + (risk*100).toFixed(0) + '%</span>'
              + '</div>'
              + '<p style="margin-top:8px;font-size:0.85rem;">' + ins.explanation + '</p>'
              + '<p style="margin-top:4px;font-size:0.85rem;color:var(--accent);">💡 ' + ins.suggestion + '</p>'
              + '</div>';
          });
        }
        html += '</div>';
        area.innerHTML = html;
      });
    }
  </script>
</body>
</html>`
