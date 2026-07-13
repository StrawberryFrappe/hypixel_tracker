<script setup lang="ts">
const modules = [
  { code: "RAW", name: "Source capture", state: "Pending", detail: "Collector not implemented" },
  { code: "CUR", name: "Curation", state: "Pending", detail: "No visible generation" },
  { code: "RANK", name: "Opportunity score", state: "Pending", detail: "No advisory output" },
];
</script>

<template>
  <div class="app-shell">
    <header class="masthead">
      <a class="brand" href="#main" aria-label="Bazaar Guru home">
        <span class="brand-mark" aria-hidden="true">BG</span>
        <span>
          <strong>BAZAAR GURU</strong>
          <small>MARKET INTELLIGENCE / FOUNDATION</small>
        </span>
      </a>
      <div class="masthead-status" role="status" aria-live="polite">
        <span class="status-pulse" aria-hidden="true"></span>
        <span><b>DEGRADED</b> NO LIVE DATA</span>
      </div>
    </header>

    <aside class="rail" aria-label="Primary navigation">
      <nav>
        <a class="active" href="#overview"><span>01</span> Overview</a>
        <span class="disabled" aria-disabled="true"><span>02</span> Products (pending)</span>
        <span class="disabled" aria-disabled="true"><span>03</span> Signals (pending)</span>
        <span class="disabled" aria-disabled="true"><span>04</span> Datasets (pending)</span>
        <span class="disabled" aria-disabled="true"><span>05</span> Operations (pending)</span>
      </nav>
      <div class="rail-note">
        <span>MODE</span>
        <strong>ADVISORY ONLY</strong>
        <p>Unofficial Hypixel tool. Never executes game actions.</p>
      </div>
    </aside>

    <main id="main">
      <section id="overview" class="hero-grid" aria-labelledby="page-title">
        <div class="hero-copy">
          <p class="eyebrow">SYSTEM / RELEASE 1 SCAFFOLD</p>
          <h1 id="page-title">Market terminal<br /><em>awaiting signal.</em></h1>
          <p class="hero-summary">
            Foundation online. Collection, curated history, and opportunity analysis remain
            intentionally unavailable until later reviewed phases.
          </p>
        </div>

        <section class="freshness-card" aria-labelledby="freshness-title">
          <div class="panel-heading">
            <span id="freshness-title">DATA FRESHNESS</span>
            <span class="tag tag-warning">UNAVAILABLE</span>
          </div>
          <div class="radar" aria-hidden="true">
            <span class="radar-ring radar-ring-one"></span>
            <span class="radar-ring radar-ring-two"></span>
            <span class="radar-cross radar-cross-x"></span>
            <span class="radar-cross radar-cross-y"></span>
            <b>--</b>
          </div>
          <dl class="freshness-stats">
            <div><dt>Latest source</dt><dd>Not captured</dd></div>
            <div><dt>Transform lag</dt><dd>Not measured</dd></div>
            <div><dt>Possible gaps</dt><dd>Unknown</dd></div>
          </dl>
        </section>
      </section>

      <section class="status-strip" aria-label="System summary">
        <div><span>API CONTRACT</span><strong>/api/v1</strong><small>Health only</small></div>
        <div><span>RAW STORE</span><strong>NO OBSERVATIONS</strong><small>Capture pending</small></div>
        <div><span>APP STORE</span><strong>NO GENERATION</strong><small>Serving pending</small></div>
        <div><span>MARKET STATE</span><strong>WITHHELD</strong><small>No stale claims</small></div>
      </section>

      <section class="workspace-grid">
        <section class="panel module-panel" aria-labelledby="module-title">
          <div class="panel-heading">
            <span id="module-title">CAPABILITY LEDGER</span>
            <span>03 TRACKS</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr><th>Track</th><th>Capability</th><th>State</th><th>Evidence</th></tr>
              </thead>
              <tbody>
                <tr v-for="module in modules" :key="module.code">
                  <td><code>{{ module.code }}</code></td>
                  <td>{{ module.name }}</td>
                  <td><span class="pending-dot"></span>{{ module.state }}</td>
                  <td>{{ module.detail }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <section class="panel guardrail-panel" aria-labelledby="guardrail-title">
          <div class="panel-heading">
            <span id="guardrail-title">PRESENTATION GUARD</span>
            <span class="tag">ACTIVE</span>
          </div>
          <div class="guardrail-copy">
            <span class="lock-glyph" aria-hidden="true">//</span>
            <h2>No fabricated market rows.</h2>
            <p>
              Prices, spreads, ranks, and charts stay hidden until a future curated generation
              provides freshness and lineage evidence.
            </p>
          </div>
          <div class="rule-list">
            <span>Stale data must identify itself</span>
            <span>Unknown gaps remain unknown</span>
            <span>Advice never becomes automation</span>
          </div>
        </section>
      </section>
    </main>

    <footer>
      <span>BAZAAR GURU v0.1.0</span>
      <span>UNOFFICIAL / NOT AFFILIATED WITH HYPIXEL</span>
    </footer>
  </div>
</template>
