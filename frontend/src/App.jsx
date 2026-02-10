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

function App() {
  const [universe, setUniverse] = useState([])
  const [filter, setFilter] = useState('KOSPI100')
  const [selected, setSelected] = useState(null)
  const [prices, setPrices] = useState([])
  const [pricesLoading, setPricesLoading] = useState(false)
  const [days, setDays] = useState(120)
  const [signals, setSignals] = useState([])
  const [status, setStatus] = useState(null)
  const [engines, setEngines] = useState(null)
  const [orders, setOrders] = useState([])
  const [positions, setPositions] = useState([])
  const [strategy, setStrategy] = useState(null)
  const [jobs, setJobs] = useState([])
  const [sectors, setSectors] = useState([])
  const [sectorFilter, setSectorFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [lastUpdated, setLastUpdated] = useState(null)

  const loadData = (sectorOverride) => {
    const sector = typeof sectorOverride === 'string' ? sectorOverride : sectorFilter
    fetchUniverse(sector && sector !== 'ALL' ? sector : undefined).then(setUniverse)
    fetchSectors().then(setSectors)
    fetchSignals().then(setSignals)
    fetchStatus().then(setStatus)
    fetchEngines().then(setEngines)
    fetchOrders().then(setOrders)
    fetchPositions().then(setPositions)
    fetchStrategy().then(setStrategy)
    fetchJobs().then(setJobs)
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

  const filtered = useMemo(() => {
    const target = universe.filter(u => u.group === filter)
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
    () => sectors.filter(s => s.market === marketFilter).sort((a, b) => b.count - a.count),
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
              <h2>Instrument</h2>
              <p>선택 종목 상세</p>
            </div>
            <div className="panel-badges">
              <span className={`status-pill ${engines?.monitor?.running ? 'on' : 'off'}`}>
                MONITOR {engines?.monitor?.running ? 'ON' : 'OFF'}
              </span>
              <span className={`status-pill ${engines?.accuracy_loader?.running ? 'on' : 'off'}`}>
                ACC {engines?.accuracy_loader?.running ? 'ON' : 'OFF'}
              </span>
            </div>
          </div>

          {selected ? (
            <div className="instrument">
              <div className="instrument-head">
                <div>
                  <div className="instrument-code">{selected.code}</div>
                  <div className="instrument-name">{selected.name}</div>
                  <div className="instrument-meta">{selected.market} · {selected.sector_name || 'UNKNOWN'}</div>
                </div>
                <div className={`delta ${delta >= 0 ? 'up' : 'down'}`}>
                  <div className="delta-value">{formatNumber(latest?.close)}</div>
                  <div className="delta-sub">{formatPct(deltaPct)}</div>
                </div>
              </div>

              <div className="stats-grid">
                <div className="stat-tile">
                  <span>Close</span>
                  <strong>{formatNumber(latest?.close)}</strong>
                  <em>MA25 {formatNumber(latest?.ma25)}</em>
                </div>
                <div className="stat-tile">
                  <span>Volume</span>
                  <strong>{formatNumber(latest?.volume)}</strong>
                  <em>Amount {formatNumber(latest?.amount)}</em>
                </div>
                <div className="stat-tile">
                  <span>Disparity</span>
                  <strong>{formatPct((latest?.disparity || 0) * 100)}</strong>
                  <em>vs MA25</em>
                </div>
                <div className="stat-tile">
                  <span>Range</span>
                  <strong>{formatNumber(latest?.high)} / {formatNumber(latest?.low)}</strong>
                  <em>Open {formatNumber(latest?.open)}</em>
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
                      <span>{formatNumber(row.open)}</span>
                      <span>{formatNumber(row.high)}</span>
                      <span>{formatNumber(row.low)}</span>
                      <span className="b">{formatNumber(row.close)}</span>
                      <span>{formatNumber(row.volume)}</span>
                      <span>{formatNumber(row.amount)}</span>
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
                    <span>{formatNumber(p.avg_price)}</span>
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
                <div><span>Order Value</span><strong>{formatNumber(strategy.order_value)}</strong></div>
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
    </div>
  )
}

export default App
