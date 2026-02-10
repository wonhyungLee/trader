import { useEffect, useMemo, useState } from 'react'
import {
  fetchUniverse,
  fetchSectors,
  fetchPrices,
  fetchSignals,
  fetchStatus,
  fetchEngines,
  fetchOrders,
  fetchPositions,
  fetchPortfolio,
  fetchPlans,
  fetchAccount,
  fetchSelection,
  fetchStrategy,
  fetchJobs
} from './api'
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceLine,
  Legend,
  AreaChart
} from 'recharts'
import './App.css'

const formatNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  if (Math.abs(num) >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(1)}B`
  if (Math.abs(num) >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (Math.abs(num) >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toLocaleString()
}

const formatCurrency = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  if (typeof Intl === 'undefined' || !Intl.NumberFormat) {
    return `₩${formatNumber(num)}`
  }
  try {
    return new Intl.NumberFormat('ko-KR', {
      style: 'currency',
      currency: 'KRW',
      maximumFractionDigits: 0
    }).format(num)
  } catch (e) {
    return `₩${formatNumber(num)}`
  }
}

const formatPct = (value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`
}

const formatTime = (value) => {
  if (!value) return '-'
  const ts = typeof value === 'string' ? value : String(value)
  if (ts.includes('T')) {
    const part = ts.split('T')[1]
    return part ? part.split('.')[0] : ts
  }
  return ts
}

const asArray = (value) => (Array.isArray(value) ? value : [])

function App() {
  const [universe, setUniverse] = useState([])
  const [filter, setFilter] = useState('KOSPI100')
  const [selected, setSelected] = useState(null)
  const [focusTab, setFocusTab] = useState('scan')
  const [prices, setPrices] = useState([])
  const [pricesLoading, setPricesLoading] = useState(false)
  const [days, setDays] = useState(120)
  const [signals, setSignals] = useState([])
  const [status, setStatus] = useState(null)
  const [engines, setEngines] = useState(null)
  const [orders, setOrders] = useState([])
  const [positions, setPositions] = useState([])
  const [portfolio, setPortfolio] = useState({ positions: [], totals: {} })
  const [plans, setPlans] = useState({ buys: [], sells: [], exec_date: null })
  const [account, setAccount] = useState(null)
  const [selection, setSelection] = useState({ stages: [], candidates: [], pricing: {} })
  const [activeStage, setActiveStage] = useState('final')
  const [strategy, setStrategy] = useState(null)
  const [jobs, setJobs] = useState([])
  const [sectors, setSectors] = useState([])
  const [sectorFilter, setSectorFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [lastUpdated, setLastUpdated] = useState(null)

  const loadData = (sectorOverride) => {
    const sector = typeof sectorOverride === 'string' ? sectorOverride : sectorFilter
    fetchUniverse(sector && sector !== 'ALL' ? sector : undefined).then((data) => setUniverse(asArray(data)))
    fetchSectors().then((data) => setSectors(asArray(data)))
    fetchSignals().then((data) => setSignals(asArray(data)))
    fetchStatus().then((data) => setStatus(data && typeof data === 'object' ? data : null))
    fetchEngines().then((data) => setEngines(data && typeof data === 'object' ? data : null))
    fetchOrders().then((data) => setOrders(asArray(data)))
    fetchPositions().then((data) => setPositions(asArray(data)))
    fetchPortfolio().then((data) => {
      const payload = data && typeof data === 'object' ? data : {}
      setPortfolio({
        positions: asArray(payload.positions),
        totals: payload.totals && typeof payload.totals === 'object' ? payload.totals : {},
      })
    })
    fetchPlans().then((data) => {
      const payload = data && typeof data === 'object' ? data : {}
      const buys = asArray(payload.buys)
      const sells = asArray(payload.sells)
      setPlans({
        ...payload,
        buys,
        sells,
        counts: payload.counts && typeof payload.counts === 'object'
          ? payload.counts
          : { buys: buys.length, sells: sells.length },
      })
    })
    fetchAccount().then((data) => setAccount(data && typeof data === 'object' ? data : null))
    fetchSelection().then((data) => {
      const payload = data && typeof data === 'object' ? data : {}
      setSelection({
        ...payload,
        stages: asArray(payload.stages),
        candidates: asArray(payload.candidates),
        pricing: payload.pricing && typeof payload.pricing === 'object' ? payload.pricing : {},
        stage_items: payload.stage_items && typeof payload.stage_items === 'object' ? payload.stage_items : {},
      })
    })
    fetchStrategy().then((data) => setStrategy(data && typeof data === 'object' ? data : null))
    fetchJobs().then((data) => setJobs(asArray(data)))
    setLastUpdated(new Date())
  }

  useEffect(() => {
    loadData()
    const id = setInterval(() => loadData(), 30000)
    return () => clearInterval(id)
  }, [sectorFilter])

  useEffect(() => {
    if (!selected) return
    setPricesLoading(true)
    fetchPrices(selected.code, days)
      .then((data) => {
        setPrices(Array.isArray(data) ? data : [])
      })
      .catch(() => {
        setPrices([])
      })
      .finally(() => setPricesLoading(false))
  }, [selected, days])

  useEffect(() => {
    setSectorFilter('ALL')
    setSelected(null)
  }, [filter])

  useEffect(() => {
    setSelected(null)
  }, [sectorFilter])

  useEffect(() => {
    const items = selection?.stage_items || {}
    if (!items || Object.keys(items).length === 0) return
    if (!items[activeStage]) {
      setActiveStage('final')
    }
  }, [selection, activeStage])

  const filtered = useMemo(() => {
    const target = asArray(universe).filter(u => u.group === filter)
    const keyword = search.trim().toLowerCase()
    if (!keyword) return target
    return target.filter(u =>
      u.code?.toLowerCase().includes(keyword) ||
      u.name?.toLowerCase().includes(keyword) ||
      u.sector_name?.toLowerCase().includes(keyword)
    )
  }, [universe, filter, search])

  const marketFilter = filter === 'KOSPI100' ? 'KOSPI' : 'KOSDAQ'
  const sectorOptions = useMemo(
    () => asArray(sectors).filter(s => s.market === marketFilter).sort((a, b) => b.count - a.count),
    [sectors, marketFilter]
  )

  const chartData = useMemo(() => [...prices].reverse(), [prices])
  const latest = chartData.length ? chartData[chartData.length - 1] : null
  const previous = chartData.length > 1 ? chartData[chartData.length - 2] : null
  const delta = latest && previous ? latest.close - previous.close : 0
  const deltaPct = latest && previous && previous.close ? (delta / previous.close) * 100 : 0

  const accuracy = status?.accuracy || {}
  const accuracyRows = [
    { label: 'INV', value: accuracy.investor_flow_daily?.missing_codes || 0 },
    { label: 'PROG', value: accuracy.program_trade_daily?.missing_codes || 0 },
    { label: 'SHORT', value: accuracy.short_sale_daily?.missing_codes || 0 },
    { label: 'CREDIT', value: accuracy.credit_balance_daily?.missing_codes || 0 },
    { label: 'LOAN', value: accuracy.loan_trans_daily?.missing_codes || 0 },
    { label: 'VI', value: accuracy.vi_status_daily?.missing_codes || 0 }
  ]

  const planBuys = asArray(plans?.buys)
  const planSells = asArray(plans?.sells)
  const accountSummary = account?.summary || {}
  const sinceConnected = account?.since_connected || {}
  const portfolioPositions = asArray(portfolio?.positions)
  const portfolioTotals = portfolio?.totals || {}
  const selectionStages = asArray(selection?.stages)
  const selectionCandidates = asArray(selection?.candidates)
  const selectionPricing = selection?.pricing || {}
  const selectionStageItems = selection?.stage_items && typeof selection.stage_items === 'object' ? selection.stage_items : {}
  const scanMode = selection?.mode || 'DAILY'
  const scanModeReason = selection?.mode_reason
  const activeStageItems = asArray(selectionStageItems[activeStage])

  const formatStageValue = (stage) => {
    if (!stage) return '-'
    if (stage.key === 'min_amount') return formatCurrency(stage.value)
    if (stage.key === 'liquidity') return `Top ${stage.value}`
    if (stage.key === 'final') return `Max ${stage.value}`
    if (stage.key === 'disparity' && stage.value) {
      const k = formatPct((stage.value.kospi || 0) * 100)
      const q = formatPct((stage.value.kosdaq || 0) * 100)
      return `KOSPI ${k} · KOSDAQ ${q}`
    }
    return stage.value ?? '-'
  }

  const lastDate = status?.daily_price?.date?.max
  const refreshLabel = lastUpdated
    ? `${lastUpdated.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' })}`
    : '-'

  const tableRows = useMemo(() => {
    if (!chartData.length) return []
    return [...chartData].reverse().slice(0, 30)
  }, [chartData])

  return (
    <div className="app-shell">
      <div className="orb orb-1" />
      <div className="orb orb-2" />
      <div className="orb orb-3" />

      <header className="hero">
        <div className="hero-main">
          <div className="hero-badge">LIVE MARKET CONSOLE</div>
          <h1>BNF-K Trading Observatory</h1>
          <p className="hero-sub">
            종목·차트·데이터 파이프라인을 한 화면에서 추적하는 실시간 관제 대시보드
          </p>
          <div className="hero-controls">
            <div className="segmented">
              <button className={filter === 'KOSPI100' ? 'active' : ''} onClick={() => setFilter('KOSPI100')}>KOSPI 100</button>
              <button className={filter === 'KOSDAQ150' ? 'active' : ''} onClick={() => setFilter('KOSDAQ150')}>KOSDAQ 150</button>
            </div>
            <div className="control">
              <label>Sector</label>
              <select value={sectorFilter} onChange={e => setSectorFilter(e.target.value)}>
                <option value="ALL">전체 섹터</option>
                {sectorOptions.map((s, i) => (
                  <option key={`${s.market}-${s.sector_name}-${i}`} value={s.sector_name}>
                    {s.sector_name} ({s.count})
                  </option>
                ))}
              </select>
            </div>
            <div className="control">
              <label>Days</label>
              <input type="number" value={days} onChange={e => setDays(Number(e.target.value) || 60)} min={10} max={400} />
            </div>
            <button className="primary-btn" onClick={() => loadData()}>Refresh</button>
          </div>
        </div>
        <div className="hero-metrics">
          <div className="metric-card">
            <span className="metric-label">Universe</span>
            <strong>{status?.universe?.total || 0} 종목</strong>
            <span className="metric-sub">{filter}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Daily Data</span>
            <strong>{status?.daily_price?.codes || 0} / 250</strong>
            <span className="metric-sub">결측 {status?.daily_price?.missing_codes || 0}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Last Update</span>
            <strong>{lastDate || '-'}</strong>
            <span className="metric-sub">refresh {refreshLabel}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Signals</span>
            <strong>{signals.length}</strong>
            <span className="metric-sub">최근 30건</span>
          </div>
        </div>
      </header>

      <main className="main-grid">
        <section className="panel universe-panel">
          <div className="panel-header">
            <div>
              <h2>Universe</h2>
              <p>{filtered.length} 종목</p>
            </div>
            <div className="search">
              <input
                placeholder="코드/종목명/섹터 검색"
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
            </div>
          </div>
          <div className="list">
            {filtered.map((row) => (
              <button
                key={row.code}
                className={`list-row ${selected?.code === row.code ? 'active' : ''}`}
                onClick={() => setSelected(row)}
              >
                <div>
                  <div className="ticker">{row.code}</div>
                  <div className="name">{row.name}</div>
                  <div className="meta">
                    <span>{row.sector_name || 'UNKNOWN'}</span>
                    {row.industry_name ? <span className="dot">•</span> : null}
                    {row.industry_name ? <span>{row.industry_name}</span> : null}
                  </div>
                </div>
                <div className="tag">{row.market}</div>
              </button>
            ))}
          </div>
        </section>

        <section className="panel focus-panel">
          <div className="panel-header">
            <div>
              <h2>Scan & Trade</h2>
              <p>{scanMode} Mode {scanModeReason ? `· ${scanModeReason}` : ''}</p>
            </div>
            <div className="panel-tabs">
              <button className={focusTab === 'scan' ? 'active' : ''} onClick={() => setFocusTab('scan')}>Scan</button>
              <button className={focusTab === 'instrument' ? 'active' : ''} onClick={() => setFocusTab('instrument')}>Instrument</button>
            </div>
          </div>

          {focusTab === 'scan' ? (
            <div className="scan-board">
              <div className="panel-badges">
                <span className={`status-pill ${engines?.monitor?.running ? 'on' : 'off'}`}>
                  MONITOR {engines?.monitor?.running ? 'ON' : 'OFF'}
                </span>
                <span className={`status-pill ${engines?.accuracy_loader?.running ? 'on' : 'off'}`}>
                  ACC {engines?.accuracy_loader?.running ? 'ON' : 'OFF'}
                </span>
                <span className="status-pill">{selection?.date || '-'}</span>
              </div>

              <div className="plan-grid">
                <div className="plan-column">
                  <div className="plan-head">Buy Candidates</div>
                  <div className="plan-list">
                    {planBuys.slice(0, 8).map((row) => (
                      <div key={row.id} className="plan-row">
                        <div>
                          <div className="mono">{row.code}</div>
                          <div className="plan-name">{row.name || '-'}</div>
                        </div>
                        <div className="plan-meta">
                          <span className="plan-price">{formatCurrency(row.planned_price)}</span>
                          <span className="plan-qty">x{formatNumber(row.qty)}</span>
                          <span className={`plan-status ${row.status?.toLowerCase()}`}>{row.status}</span>
                        </div>
                      </div>
                    ))}
                    {planBuys.length === 0 && <div className="empty">매수 예정 종목이 없습니다.</div>}
                  </div>
                </div>
                <div className="plan-column">
                  <div className="plan-head">Sell Candidates</div>
                  <div className="plan-list">
                    {planSells.slice(0, 8).map((row) => (
                      <div key={row.id} className="plan-row">
                        <div>
                          <div className="mono">{row.code}</div>
                          <div className="plan-name">{row.name || '-'}</div>
                        </div>
                        <div className="plan-meta">
                          <span className="plan-price">{formatCurrency(row.planned_price)}</span>
                          <span className="plan-qty">x{formatNumber(row.qty)}</span>
                          <span className={`plan-status ${row.status?.toLowerCase()}`}>{row.status}</span>
                        </div>
                      </div>
                    ))}
                    {planSells.length === 0 && <div className="empty">매도 예정 종목이 없습니다.</div>}
                  </div>
                </div>
              </div>

              <div className="pipeline-flow">
                {selectionStages.map((stage, idx) => {
                  const isActive = activeStage === stage.key
                  return (
                    <button
                      key={stage.key}
                      className={`pipeline-step ${isActive ? 'active' : ''}`}
                      onClick={() => setActiveStage(stage.key)}
                      type="button"
                    >
                      <div className="pipeline-label">{stage.label}</div>
                      <div className="pipeline-count">{stage.count}</div>
                      <div className="pipeline-value">{formatStageValue(stage)}</div>
                      <div className="pipeline-bar">
                        <span style={{ width: selectionStages[0]?.count ? `${(stage.count / selectionStages[0].count) * 100}%` : '0%' }} />
                      </div>
                      {idx < selectionStages.length - 1 && <div className="pipeline-arrow">→</div>}
                    </button>
                  )
                })}
                {selectionStages.length === 0 && <div className="empty">선정 파이프라인 데이터를 불러오지 못했습니다.</div>}
              </div>

              <div className="pricing-box">
                <div className="pricing-title">Price Decision Logic</div>
                <div className="pricing-grid">
                  <div>
                    <span>Price Source</span>
                    <strong>{selectionPricing.price_source || 'close'}</strong>
                  </div>
                  <div>
                    <span>Order Value</span>
                    <strong>{formatCurrency(selectionPricing.order_value)}</strong>
                  </div>
                  <div>
                    <span>Qty Formula</span>
                    <strong>{selectionPricing.qty_formula || 'order_value / close'}</strong>
                  </div>
                  <div>
                    <span>Order Type</span>
                    <strong>{selectionPricing.ord_dvsn || '-'}</strong>
                  </div>
                  <div>
                    <span>Sell Take Profit</span>
                    <strong>{formatPct((selectionPricing.sell_rules?.take_profit_disparity || 0) * 100)}</strong>
                  </div>
                  <div>
                    <span>Sell Stop Loss</span>
                    <strong>{formatPct((selectionPricing.sell_rules?.stop_loss || 0) * 100)}</strong>
                  </div>
                  <div>
                    <span>Max Holding</span>
                    <strong>{selectionPricing.sell_rules?.max_holding_days || '-'} days</strong>
                  </div>
                </div>
              </div>

              <div className="candidate-table">
                <div className="candidate-row head">
                  <span>Rank</span>
                  <span>Code</span>
                  <span>Name</span>
                  <span>Amount</span>
                  <span>Disparity</span>
                  <span>Close</span>
                </div>
                {selectionCandidates.slice(0, 12).map((row) => (
                  <div key={row.code} className="candidate-row">
                    <span>{row.rank}</span>
                    <span className="mono">{row.code}</span>
                    <span>{row.name}</span>
                    <span>{formatCurrency(row.amount)}</span>
                    <span className={row.disparity <= 0 ? 'down' : 'up'}>{formatPct((row.disparity || 0) * 100)}</span>
                    <span>{formatCurrency(row.close)}</span>
                  </div>
                ))}
                {selectionCandidates.length === 0 && <div className="empty">현재 선정된 후보가 없습니다.</div>}
              </div>

              <div className="stage-table">
                <div className="stage-title">
                  Stage Detail: {selectionStages.find(s => s.key === activeStage)?.label || 'N/A'}
                </div>
                <div className="stage-row head">
                  <span>Code</span>
                  <span>Name</span>
                  <span>Amount</span>
                  <span>Disparity</span>
                  <span>Close</span>
                </div>
                {activeStageItems.slice(0, 12).map((row) => (
                  <div key={`${activeStage}-${row.code}`} className="stage-row">
                    <span className="mono">{row.code}</span>
                    <span>{row.name}</span>
                    <span>{formatCurrency(row.amount)}</span>
                    <span className={row.disparity <= 0 ? 'down' : 'up'}>{formatPct((row.disparity || 0) * 100)}</span>
                    <span>{formatCurrency(row.close)}</span>
                  </div>
                ))}
                {activeStageItems.length === 0 && (
                  <div className="empty">해당 단계에 표시할 종목이 없습니다.</div>
                )}
              </div>
            </div>
          ) : selected ? (
            <div className="instrument">
              <div className="instrument-head">
                <div>
                  <div className="instrument-code">{selected.code}</div>
                  <div className="instrument-name">{selected.name}</div>
                  <div className="instrument-meta">{selected.market} · {selected.sector_name || 'UNKNOWN'}</div>
                </div>
                <div className={`delta ${delta >= 0 ? 'up' : 'down'}`}>
                  <div className="delta-value">{formatCurrency(latest?.close)}</div>
                  <div className="delta-sub">{formatPct(deltaPct)}</div>
                </div>
              </div>

              <div className="stats-grid">
                <div className="stat-tile">
                  <span>Close</span>
                  <strong>{formatCurrency(latest?.close)}</strong>
                  <em>MA25 {formatCurrency(latest?.ma25)}</em>
                </div>
                <div className="stat-tile">
                  <span>Volume</span>
                  <strong>{formatNumber(latest?.volume)}</strong>
                  <em>Amount {formatCurrency(latest?.amount)}</em>
                </div>
                <div className="stat-tile">
                  <span>Disparity</span>
                  <strong>{formatPct((latest?.disparity || 0) * 100)}</strong>
                  <em>vs MA25</em>
                </div>
                <div className="stat-tile">
                  <span>Range</span>
                  <strong>{formatCurrency(latest?.high)} / {formatCurrency(latest?.low)}</strong>
                  <em>Open {formatCurrency(latest?.open)}</em>
                </div>
              </div>

              <div className="chart-stack">
                <div className="chart-card">
                  <div className="chart-title">Price · MA25 · Volume</div>
                  {pricesLoading ? (
                    <div className="empty">차트 로딩 중...</div>
                  ) : chartData.length === 0 ? (
                    <div className="empty">가격 데이터가 없습니다.</div>
                  ) : (
                    <ResponsiveContainer width="100%" height={260}>
                      <ComposedChart data={chartData} margin={{ left: 6, right: 18, top: 10, bottom: 8 }}>
                        <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.08)" />
                        <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#94a3b8' }} interval={Math.max(0, Math.floor(chartData.length / 6))} />
                        <YAxis yAxisId="price" tick={{ fontSize: 11, fill: '#94a3b8' }} domain={['auto', 'auto']} />
                        <YAxis yAxisId="volume" orientation="right" tick={{ fontSize: 11, fill: '#94a3b8' }} />
                        <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid rgba(148,163,184,0.3)' }} labelStyle={{ color: '#e2e8f0' }} />
                        <Legend wrapperStyle={{ color: '#cbd5f5' }} />
                        <Area yAxisId="price" dataKey="close" stroke="#7dd3fc" fill="rgba(14,116,144,0.35)" name="Close" />
                        <Line yAxisId="price" type="monotone" dataKey="ma25" stroke="#facc15" dot={false} strokeWidth={2} name="MA25" />
                        <Bar yAxisId="volume" dataKey="volume" fill="rgba(251,191,36,0.35)" name="Volume" />
                      </ComposedChart>
                    </ResponsiveContainer>
                  )}
                </div>
                <div className="chart-card">
                  <div className="chart-title">Disparity Flow</div>
                  {chartData.length === 0 ? (
                    <div className="empty">가격 데이터가 없습니다.</div>
                  ) : (
                    <ResponsiveContainer width="100%" height={160}>
                      <AreaChart data={chartData} margin={{ left: 6, right: 18, top: 10, bottom: 8 }}>
                        <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.08)" />
                        <XAxis dataKey="date" hide />
                        <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} />
                        <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid rgba(148,163,184,0.3)' }} labelStyle={{ color: '#e2e8f0' }} />
                        <ReferenceLine y={0} stroke="#94a3b8" strokeDasharray="4 4" />
                        <Area dataKey="disparity" stroke="#fb7185" fill="rgba(248,113,113,0.35)" />
                      </AreaChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </div>

              <div className="panel soft">
                <div className="panel-title">Daily Prices</div>
                <div className="price-table">
                  <div className="price-row head">
                    <span>Date</span>
                    <span>Open</span>
                    <span>High</span>
                    <span>Low</span>
                    <span>Close</span>
                    <span>Volume</span>
                    <span>Amount</span>
                    <span>Disp</span>
                  </div>
                  {tableRows.map((row) => (
                    <div key={row.date} className="price-row">
                      <span className="mono">{row.date}</span>
                      <span>{formatCurrency(row.open)}</span>
                      <span>{formatCurrency(row.high)}</span>
                      <span>{formatCurrency(row.low)}</span>
                      <span className="b">{formatCurrency(row.close)}</span>
                      <span>{formatNumber(row.volume)}</span>
                      <span>{formatCurrency(row.amount)}</span>
                      <span>{formatPct((row.disparity || 0) * 100)}</span>
                    </div>
                  ))}
                  {tableRows.length === 0 && <div className="empty">가격 데이터가 없습니다.</div>}
                </div>
              </div>

            </div>
          ) : (
            <div className="placeholder">왼쪽에서 종목을 선택하세요.</div>
          )}

          <div className="split-grid">
            <div className="panel subtle">
              <div className="panel-title">Recent Orders</div>
              <div className="mini-table">
                {orders.slice(0, 6).map((o, i) => (
                  <div key={i} className="mini-row">
                    <span className="mono">{o.code}</span>
                    <span className={`side ${o.side === 'BUY' ? 'buy' : 'sell'}`}>{o.side}</span>
                    <span>{formatNumber(o.qty)}</span>
                    <span>{o.status}</span>
                  </div>
                ))}
                {orders.length === 0 && <div className="empty">주문 내역이 없습니다.</div>}
              </div>
            </div>
            <div className="panel subtle">
              <div className="panel-title">Positions</div>
              <div className="mini-table">
                {positions.slice(0, 6).map((p, i) => (
                  <div key={i} className="mini-row">
                    <span className="mono">{p.code}</span>
                    <span>{p.qty}</span>
                    <span>{formatCurrency(p.avg_price)}</span>
                    <span>{formatTime(p.updated_at)}</span>
                  </div>
                ))}
                {positions.length === 0 && <div className="empty">보유 포지션이 없습니다.</div>}
              </div>
            </div>
          </div>
        </section>

        <section className="panel side-panel">
          <div className="panel-header">
            <div>
              <h2>Telemetry</h2>
              <p>엔진 · 데이터 · 시그널</p>
            </div>
          </div>

          <div className="engine-grid">
            <div className={`engine-card ${engines?.monitor?.running ? 'on' : 'off'}`}>
              <span>Monitoring</span>
              <strong>{engines?.monitor?.running ? 'RUNNING' : 'STOP'}</strong>
            </div>
            <div className="engine-card on">
              <span>Trader</span>
              <strong>P:{engines?.trader?.pending || 0} S:{engines?.trader?.sent || 0}</strong>
            </div>
            <div className={`engine-card ${engines?.accuracy_loader?.running ? 'on' : 'off'}`}>
              <span>Accuracy</span>
              <strong>{engines?.accuracy_loader?.running ? 'RUNNING' : 'STOP'}</strong>
            </div>
          </div>

          <div className="panel soft">
            <div className="panel-title">Engine Snapshot</div>
            <div className="engine-snapshot">
              <div>
                <span>Selection Signals</span>
                <strong>{plans?.counts?.buys || 0} BUY / {plans?.counts?.sells || 0} SELL</strong>
                <em>exec {plans?.exec_date || '-'}</em>
              </div>
              <div>
                <span>Trader Queue</span>
                <strong>P:{engines?.trader?.pending || 0} S:{engines?.trader?.sent || 0}</strong>
                <em>last {engines?.trader?.last_signal || '-'}</em>
              </div>
              <div>
                <span>Connected</span>
                <strong>{account?.connected ? 'YES' : 'NO'}</strong>
                <em>{account?.connected_at ? new Date(account.connected_at).toLocaleString('ko-KR') : '-'}</em>
              </div>
            </div>
          </div>

          <div className="panel soft">
            <div className="panel-title">Data Health</div>
            <div className="health-grid">
              <div>
                <span>일봉 커버리지</span>
                <strong>{status?.daily_price?.codes || 0} / 250</strong>
              </div>
              <div>
                <span>결측</span>
                <strong>{status?.daily_price?.missing_codes || 0}</strong>
              </div>
            </div>
            <div className="accuracy-list">
              {accuracyRows.map((row) => (
                <div key={row.label} className="accuracy-row">
                  <div className="label">{row.label}</div>
                  <div className="bar">
                    <span style={{ width: `${Math.min(100, (row.value / 250) * 100)}%` }} />
                  </div>
                  <div className="value">{row.value}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="panel soft">
            <div className="panel-title">Signal Feed</div>
            <div className="signal-list">
              {signals.slice(0, 8).map((s, i) => (
                <div key={i} className="signal-row">
                  <span className={`signal-side ${s.side === 'BUY' ? 'buy' : 'sell'}`}>{s.side}</span>
                  <span className="mono">{s.code}</span>
                  <span>{formatNumber(s.qty)}</span>
                  <span>{s.signal_date}</span>
                </div>
              ))}
              {signals.length === 0 && <div className="empty">최근 시그널이 없습니다.</div>}
            </div>
          </div>

          <div className="panel soft">
            <div className="panel-title">Strategy</div>
            {strategy ? (
              <div className="strategy-grid">
                <div><span>Disparity KOSPI</span><strong>{strategy.disparity_buy_kospi}</strong></div>
                <div><span>Disparity KOSDAQ</span><strong>{strategy.disparity_buy_kosdaq}</strong></div>
                <div><span>Disparity Sell</span><strong>{strategy.disparity_sell}</strong></div>
                <div><span>Stop Loss</span><strong>{strategy.stop_loss}</strong></div>
                <div><span>Order Value</span><strong>{formatCurrency(strategy.order_value)}</strong></div>
                <div><span>Max Holding</span><strong>{strategy.max_holding_days}</strong></div>
              </div>
            ) : (
              <div className="empty">전략 정보를 불러오지 못했습니다.</div>
            )}
          </div>

          <div className="panel soft">
            <div className="panel-title">Recent Jobs</div>
            <div className="job-list">
              {jobs.slice(0, 6).map((j, i) => (
                <div key={i} className="job-row">
                  <div>
                    <strong>{j.job_name}</strong>
                    <span>{formatTime(j.started_at)}</span>
                  </div>
                  <span className={`job-status ${j.status === 'SUCCESS' ? 'ok' : 'err'}`}>{j.status}</span>
                </div>
              ))}
              {jobs.length === 0 && <div className="empty">작업 로그가 없습니다.</div>}
            </div>
          </div>
        </section>
      </main>

      <section className="wide-grid">
        <div className="panel soft">
          <div className="panel-title">Account Overview</div>
          {account?.connected ? (
            <>
              <div className="account-grid">
                <div>
                  <span>Total Assets</span>
                  <strong>{formatCurrency(accountSummary.total_assets)}</strong>
                </div>
                <div>
                  <span>Cash</span>
                  <strong>{formatCurrency(accountSummary.cash)}</strong>
                </div>
                <div>
                  <span>Positions Value</span>
                  <strong>{formatCurrency(accountSummary.positions_value)}</strong>
                </div>
                <div>
                  <span>Total PnL</span>
                  <strong className={accountSummary.total_pnl >= 0 ? 'up' : 'down'}>
                    {formatCurrency(accountSummary.total_pnl)}
                  </strong>
                  <em>{formatPct(accountSummary.total_pnl_pct)}</em>
                </div>
                <div>
                  <span>Since Connected</span>
                  <strong className={sinceConnected.pnl >= 0 ? 'up' : 'down'}>
                    {formatCurrency(sinceConnected.pnl)}
                  </strong>
                  <em>{formatPct(sinceConnected.pnl_pct)}</em>
                </div>
              </div>
              <div className="account-sub">
                연결 시점: {account?.connected_at ? new Date(account.connected_at).toLocaleString('ko-KR') : '-'}
              </div>
            </>
          ) : (
            <div className="empty">자동매매 계좌 연결 정보를 불러오지 못했습니다.</div>
          )}
        </div>

        <div className="panel soft">
          <div className="panel-title">Portfolio</div>
          <div className="portfolio-table">
            <div className="portfolio-row head">
              <span>Code</span>
              <span>Name</span>
              <span>Qty</span>
              <span>Avg</span>
              <span>Last</span>
              <span>PnL</span>
              <span>PnL%</span>
            </div>
            {portfolioPositions.slice(0, 20).map((row) => (
              <div key={row.code} className="portfolio-row">
                <span className="mono">{row.code}</span>
                <span>{row.name}</span>
                <span>{formatNumber(row.qty)}</span>
                <span>{formatCurrency(row.avg_price)}</span>
                <span>{formatCurrency(row.last_close)}</span>
                <span className={row.pnl >= 0 ? 'up' : 'down'}>{formatCurrency(row.pnl)}</span>
                <span className={row.pnl_pct >= 0 ? 'up' : 'down'}>{formatPct(row.pnl_pct)}</span>
              </div>
            ))}
            {portfolioPositions.length === 0 && <div className="empty">보유 종목이 없습니다.</div>}
          </div>
          <div className="portfolio-total">
            <span>Total</span>
            <strong>{formatCurrency(portfolioTotals.positions_value)}</strong>
            <span className={portfolioTotals.pnl >= 0 ? 'up' : 'down'}>
              {formatCurrency(portfolioTotals.pnl)} ({formatPct(portfolioTotals.pnl_pct)})
            </span>
          </div>
        </div>

      </section>
    </div>
  )
}

export default App
