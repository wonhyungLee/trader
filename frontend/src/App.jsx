import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  fetchUniverse,
  fetchSectors,
  fetchPrices,
  fetchPlans,
  fetchAccount,
  fetchSelection,
  fetchSelectionFilters,
  fetchPortfolio,
  fetchKisKeys,
  updateKisKeyToggle,
  updateSelectionFilterToggle
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
  AreaChart,
  Brush
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
  const [prices, setPrices] = useState([])
  const [pricesLoading, setPricesLoading] = useState(false)
  const [days, setDays] = useState(120)
  const [plans, setPlans] = useState({ buys: [], sells: [], exec_date: null })
  const [account, setAccount] = useState(null)
  const [selection, setSelection] = useState({ stages: [], candidates: [], pricing: {} })
  const [portfolio, setPortfolio] = useState({ positions: [], totals: {} })
  const [sectors, setSectors] = useState([])
  const [sectorFilter, setSectorFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [lastUpdated, setLastUpdated] = useState(null)
  const [kisKeys, setKisKeys] = useState([])
  const [kisError, setKisError] = useState('')
  const [filterToggles, setFilterToggles] = useState({ min_amount: true, liquidity: true, disparity: true })
  const [filterError, setFilterError] = useState('')
  const [modalOpen, setModalOpen] = useState(false)
  const [zoomRange, setZoomRange] = useState({ start: 0, end: 0 })
  const [zoomArmed, setZoomArmed] = useState(false)
  const chartWheelRef = useRef(null)
  const [openHelp, setOpenHelp] = useState(null)

  const loadData = (sectorOverride) => {
    const sector = typeof sectorOverride === 'string' ? sectorOverride : sectorFilter
    fetchUniverse(sector && sector !== 'ALL' ? sector : undefined).then((data) => setUniverse(asArray(data)))
    fetchSectors().then((data) => setSectors(asArray(data)))
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
      if (payload.filter_toggles && typeof payload.filter_toggles === 'object') {
        setFilterToggles({
          min_amount: payload.filter_toggles.min_amount !== false,
          liquidity: payload.filter_toggles.liquidity !== false,
          disparity: payload.filter_toggles.disparity !== false,
        })
      }
    })
    fetchSelectionFilters().then((data) => {
      const payload = data && typeof data === 'object' ? data : {}
      setFilterToggles({
        min_amount: payload.min_amount !== false,
        liquidity: payload.liquidity !== false,
        disparity: payload.disparity !== false,
      })
    }).catch(() => {})
    fetchKisKeys().then((data) => setKisKeys(asArray(data))).catch(() => setKisKeys([]))
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
    document.body.style.overflow = modalOpen ? 'hidden' : ''
    return () => {
      document.body.style.overflow = ''
    }
  }, [modalOpen])

  useEffect(() => {
    if (!modalOpen) setZoomArmed(false)
  }, [modalOpen])

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

  useEffect(() => {
    if (!chartData.length) return
    setZoomRange({ start: 0, end: chartData.length - 1 })
  }, [chartData.length, modalOpen])

  const planBuys = asArray(plans?.buys)
  const accountSummary = account?.summary || {}
  const portfolioPositions = asArray(portfolio?.positions)
  const portfolioTotals = portfolio?.totals || {}
  const selectionStages = asArray(selection?.stages)
  const selectionPricing = selection?.pricing || {}
  const selectionStageItems = selection?.stage_items && typeof selection.stage_items === 'object' ? selection.stage_items : {}

  const expectedReturn = Number(selectionPricing.sell_rules?.take_profit_ret)
  const expectedReturnPct = Number.isFinite(expectedReturn) ? expectedReturn * 100 : null

  const handleKisToggle = async (row) => {
    const password = window.prompt('KIS 계좌 토글 비밀번호를 입력하세요')
    if (!password) return
    try {
      const updated = await updateKisKeyToggle(row.id, !row.enabled, password)
      setKisKeys(asArray(updated))
      setKisError('')
    } catch (e) {
      setKisError('비밀번호가 올바르지 않거나 서버 오류가 발생했습니다.')
    }
  }

  const handleFilterToggle = async (key) => {
    const password = window.prompt('필터 토글 비밀번호를 입력하세요')
    if (!password) return
    try {
      const updated = await updateSelectionFilterToggle(key, !filterToggles[key], password)
      setFilterToggles({
        min_amount: updated.min_amount !== false,
        liquidity: updated.liquidity !== false,
        disparity: updated.disparity !== false,
      })
      setFilterError('')
      loadData()
    } catch (e) {
      setFilterError('비밀번호가 올바르지 않거나 서버 오류가 발생했습니다.')
    }
  }

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

  const stageOrder = ['universe', 'min_amount', 'liquidity', 'disparity', 'final']
  const stageTagMap = {
    min_amount: 'Filter 1',
    liquidity: 'Filter 2',
    disparity: 'Filter 3',
    final: 'Final'
  }

  const stageHelp = {
    min_amount: '최근 거래대금이 일정 기준 이상인 종목만 남깁니다. 거래가 활발한 종목만 먼저 걸러내는 단계입니다.',
    liquidity: '거래대금 상위 순위만 선택합니다. 사고팔기 쉬운(유동성 높은) 종목을 우선으로 봅니다.',
    disparity: '현재 가격이 이동평균(MA25) 대비 얼마나 낮거나 높은지 확인합니다. 기준값에 맞는 종목만 통과합니다.',
    final: '모든 필터를 통과한 종목 중 최대 보유 종목 수만큼 최종 후보로 선택합니다.'
  }

  const toggleHelp = (key) => {
    setOpenHelp(prev => (prev === key ? null : key))
  }

  const stageMap = useMemo(() => {
    const map = {}
    selectionStages.forEach((stage) => {
      if (stage?.key) map[stage.key] = stage
    })
    return map
  }, [selectionStages])

  const universeCount = stageMap.universe?.count || 0
  const stageNodes = stageOrder.map((key, idx) => {
    const stage = stageMap[key] || { key, label: key, count: 0, value: null }
    const prevKey = idx > 0 ? stageOrder[idx - 1] : null
    const prevCount = prevKey ? (stageMap[prevKey]?.count || 0) : stage.count || 0
    const count = stage.count || 0
    const drop = idx === 0 ? 0 : Math.max(prevCount - count, 0)
    const passRate = idx === 0 ? 1 : (prevCount ? count / prevCount : 0)
    const ratio = universeCount ? count / universeCount : 0
    const items = key === 'universe' ? [] : asArray(selectionStageItems[key])
    return {
      key,
      label: stage.label || key,
      criteria: formatStageValue(stage),
      count,
      drop,
      passRate,
      ratio,
      tag: stageTagMap[key] || '',
      items,
    }
  })

  const stageColumns = stageNodes.filter((node) => ['min_amount', 'liquidity', 'disparity'].includes(node.key))
  const finalStage = stageNodes.find((node) => node.key === 'final')

  const refreshLabel = lastUpdated
    ? `${lastUpdated.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' })}`
    : '-'

  const zoomedData = useMemo(() => {
    if (!chartData.length) return []
    const safeStart = Math.max(0, Math.min(zoomRange.start, chartData.length - 1))
    const safeEnd = Math.max(safeStart, Math.min(zoomRange.end, chartData.length - 1))
    return chartData.slice(safeStart, safeEnd + 1)
  }, [chartData, zoomRange])

  const tableRows = useMemo(() => {
    const data = zoomedData.length ? zoomedData : chartData
    if (!data.length) return []
    return [...data].reverse().slice(0, 30)
  }, [chartData, zoomedData])

  const expectedSellPrice = (price) => {
    const p = Number(price)
    if (!Number.isFinite(p) || !Number.isFinite(expectedReturn)) return null
    return p * (1 + expectedReturn)
  }

  const rangeOptions = [
    { label: '1Y', value: 252 },
    { label: '5Y', value: 252 * 5 },
    { label: '10Y', value: 252 * 10 },
    { label: 'MAX', value: 5000 }
  ]

  const handleChartWheel = (event) => {
    if (!chartData.length) return
    if (!zoomArmed) return
    event.preventDefault()
    event.stopPropagation()
    const span = Math.max(1, zoomRange.end - zoomRange.start + 1)
    const direction = event.deltaY > 0 ? 1 : -1
    const delta = Math.max(1, Math.round(span * 0.15))
    let nextSpan = span + (direction > 0 ? delta : -delta)
    const minSpan = 20
    const maxSpan = chartData.length
    nextSpan = Math.min(maxSpan, Math.max(minSpan, nextSpan))
    const rect = event.currentTarget.getBoundingClientRect()
    const ratio = rect.width ? (event.clientX - rect.left) / rect.width : 0.5
    const anchor = zoomRange.start + Math.round(span * ratio)
    let newStart = Math.round(anchor - nextSpan * ratio)
    let newEnd = newStart + nextSpan - 1
    if (newStart < 0) {
      newStart = 0
      newEnd = nextSpan - 1
    }
    if (newEnd > chartData.length - 1) {
      newEnd = chartData.length - 1
      newStart = Math.max(0, newEnd - nextSpan + 1)
    }
    setZoomRange({ start: newStart, end: newEnd })
  }

  const handleChartWheelCallback = useCallback(handleChartWheel, [chartData, zoomRange, zoomArmed])

  useEffect(() => {
    const el = chartWheelRef.current
    if (!el) return
    const onWheel = (event) => handleChartWheelCallback(event)
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
  }, [handleChartWheelCallback])

  const handleBrushChange = (range) => {
    if (!range || range.startIndex == null || range.endIndex == null) return
    setZoomRange({ start: range.startIndex, end: range.endIndex })
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-kicker">REAL TRADE ONLY</span>
          <h1 className="brand-title">BNF-K Trade Studio</h1>
          <p className="brand-sub">필터링부터 매수·매도까지 필요한 데이터만 집중 노출합니다.</p>
        </div>
        <div className="controls">
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
          <div className="refresh-meta">최근 업데이트 {refreshLabel}</div>
        </div>
      </header>

      <section id="summary" className="summary-strip">
        <div className="summary-card">
          <span>총자산</span>
          <strong>{formatCurrency(accountSummary.total_assets)}</strong>
          <em>계좌 {account?.connected ? '연결됨' : '미연결'}</em>
        </div>
        <div className="summary-card">
          <span>현금</span>
          <strong>{formatCurrency(accountSummary.cash)}</strong>
          <em>잔고 조회</em>
        </div>
        <div className="summary-card">
          <span>보유자산</span>
          <strong>{formatCurrency(accountSummary.positions_value)}</strong>
          <em>보유 평가액</em>
        </div>
        <div className="summary-card">
          <span>수익률</span>
          <strong className={accountSummary.total_pnl >= 0 ? 'up' : 'down'}>
            {formatCurrency(accountSummary.total_pnl)}
          </strong>
          <em>{formatPct(accountSummary.total_pnl_pct)}</em>
        </div>
        <div className="summary-card">
          <span>기대 수익률</span>
          <strong>{expectedReturnPct === null ? '-' : `${expectedReturnPct.toFixed(2)}%`}</strong>
          <em>자동매매 기준</em>
        </div>
      </section>

      <nav className="section-nav">
        <a href="#stocks">주식목록</a>
        <a href="#filters">선별 과정</a>
        <a href="#plans">매수 예상</a>
        <a href="#results">자동매매 결과</a>
        <a href="#account">계좌/수익률</a>
      </nav>

      <main className="layout">
        <aside id="stocks" className="panel stock-panel">
          <div className="panel-head">
            <div>
              <h2>주식목록</h2>
              <p>{filtered.length} 종목</p>
            </div>
          </div>
          <div className="search">
            <input
              placeholder="코드/종목명/섹터 검색"
              value={search}
              onChange={e => setSearch(e.target.value)}
            />
          </div>
          <div className="list">
            {filtered.map((row) => (
              <button
                key={row.code}
                className={`list-row ${selected?.code === row.code ? 'active' : ''}`}
                onClick={() => {
                  setSelected(row)
                  setModalOpen(true)
                }}
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
        </aside>

        <section className="content-column">
          <section id="filters" className="panel section">
            <div className="section-head">
              <div>
                <h2>주식 종목 선별 과정</h2>
                <p>필터 1 ~ 3 통과 종목을 단계별로 확인합니다.</p>
              </div>
              <span className="section-meta">기준일 {selection?.date || '-'}</span>
            </div>
            {filterError ? <div className="error-banner">{filterError}</div> : null}
            <div className="flow-grid">
              {stageNodes.map((stage) => (
                <div key={stage.key} className="flow-card">
                  <div className="flow-header">
                    <span>{stage.label}</span>
                    <strong>{stage.count}</strong>
                  </div>
                  <div className="flow-meta">
                    <span>{stage.criteria}</span>
                    {stage.key !== 'universe' ? (
                      <em>통과 {(stage.passRate * 100).toFixed(1)}% · 탈락 {stage.drop}</em>
                    ) : (
                      <em>기준 유니버스</em>
                    )}
                  </div>
                  <div className="flow-bar">
                    <span style={{ width: `${Math.max(6, stage.ratio * 100)}%` }} />
                  </div>
                </div>
              ))}
            </div>

            <div className="filter-columns">
              {stageColumns.map((stage) => (
                <div key={stage.key} className={`filter-column ${filterToggles[stage.key] === false ? 'disabled' : ''}`}>
                  <div className="filter-head">
                    <div>
                      <div className="filter-tag">{stage.tag}</div>
                      <div className="filter-title-row">
                        <div className="filter-title">{stage.label}</div>
                        <button
                          type="button"
                          className="help-icon"
                          aria-label={`${stage.label} 설명`}
                          aria-expanded={openHelp === stage.key}
                          onClick={() => toggleHelp(stage.key)}
                        >
                          ?
                        </button>
                        <button
                          type="button"
                          className={`filter-toggle ${filterToggles[stage.key] === false ? 'off' : 'on'}`}
                          onClick={() => handleFilterToggle(stage.key)}
                        >
                          {filterToggles[stage.key] === false ? 'OFF' : 'ON'}
                        </button>
                      </div>
                      {openHelp === stage.key ? (
                        <div className="help-bubble">{stageHelp[stage.key]}</div>
                      ) : null}
                      <div className="filter-criteria">{stage.criteria}</div>
                    </div>
                    <div className="filter-count">{stage.count}</div>
                  </div>
                  <div className="filter-sub">
                    통과 {(stage.passRate * 100).toFixed(1)}% · 탈락 {stage.drop}
                  </div>
                  <div className="filter-list">
                    {stage.items.map((row, idx) => (
                      <div key={`${stage.key}-${row.code}-${idx}`} className="filter-row">
                        <div>
                          <div className="mono">{row.code}</div>
                          <div className="filter-name">{row.name || '-'}</div>
                        </div>
                        <div className="filter-meta">
                          <span>{formatCurrency(row.amount)}</span>
                          <span className={(row.disparity ?? 0) <= 0 ? 'down' : 'up'}>
                            {formatPct((row.disparity || 0) * 100)}
                          </span>
                        </div>
                      </div>
                    ))}
                    {stage.items.length === 0 && <div className="empty">통과 종목이 없습니다.</div>}
                  </div>
                </div>
              ))}
            </div>

            {finalStage ? (
              <div className="final-board">
                <div className="final-head">
                  <div>
                    <div className="filter-tag">Final</div>
                    <div className="filter-title-row">
                      <div className="final-title">{finalStage.label}</div>
                      <button
                        type="button"
                        className="help-icon"
                        aria-label="Final 설명"
                        aria-expanded={openHelp === 'final'}
                        onClick={() => toggleHelp('final')}
                      >
                        ?
                      </button>
                    </div>
                    {openHelp === 'final' ? (
                      <div className="help-bubble">{stageHelp.final}</div>
                    ) : null}
                    <div className="filter-criteria">{finalStage.criteria}</div>
                  </div>
                  <div className="filter-count">{finalStage.count}</div>
                </div>
                <div className="final-list">
                  {finalStage.items.map((row, idx) => (
                    <div key={`final-${row.code}-${idx}`} className="final-row">
                      <div>
                        <div className="mono">{row.code}</div>
                        <div className="filter-name">{row.name || '-'}</div>
                      </div>
                      <div className="filter-meta">
                        <span>{formatCurrency(row.amount)}</span>
                        <span className={(row.disparity ?? 0) <= 0 ? 'down' : 'up'}>
                          {formatPct((row.disparity || 0) * 100)}
                        </span>
                      </div>
                    </div>
                  ))}
                  {finalStage.items.length === 0 && <div className="empty">최종 후보가 없습니다.</div>}
                </div>
              </div>
            ) : null}
          </section>

          

          <section id="plans" className="panel section">
            <div className="section-head">
              <div>
                <h2>자동매매 매수 예상</h2>
                <p>계획된 매수 가격과 기대 수익률 기준을 표시합니다.</p>
              </div>
              <span className="section-meta">기대 수익률 {expectedReturnPct === null ? '-' : `${expectedReturnPct.toFixed(2)}%`}</span>
            </div>
            <div className="plan-list">
              {planBuys.slice(0, 12).map((row) => (
                <div key={row.id} className="plan-row">
                  <div>
                    <div className="mono">{row.code}</div>
                    <div className="plan-name">{row.name || '-'}</div>
                  </div>
                  <div className="plan-meta">
                    <span className="plan-price">{formatCurrency(row.planned_price)}</span>
                    <span className="plan-qty">x{formatNumber(row.qty)}</span>
                    <span className="plan-qty">예상 매도가 {formatCurrency(expectedSellPrice(row.planned_price))}</span>
                  </div>
                </div>
              ))}
              {planBuys.length === 0 && <div className="empty">매수 예정 종목이 없습니다.</div>}
            </div>
          </section>

          <section id="results" className="panel section">
            <div className="section-head">
              <div>
                <h2>자동매매 결과</h2>
                <p>보유 포지션 기준으로 매수가격, 현재 매도가격, 수익률을 표시합니다.</p>
              </div>
              <span className="section-meta">총 {portfolioPositions.length}건</span>
            </div>
            <div className="result-table">
              <div className="result-row head">
                <span>Code</span>
                <span>Name</span>
                <span>매수가</span>
                <span>매도가격(현재)</span>
                <span>수익률</span>
                <span>Updated</span>
              </div>
              {portfolioPositions.slice(0, 14).map((row) => (
                <div key={row.code} className="result-row">
                  <span className="mono">{row.code}</span>
                  <span>{row.name}</span>
                  <span>{formatCurrency(row.avg_price)}</span>
                  <span>{formatCurrency(row.last_close)}</span>
                  <span className={row.pnl_pct >= 0 ? 'up' : 'down'}>{formatPct(row.pnl_pct)}</span>
                  <span>{formatTime(row.updated_at)}</span>
                </div>
              ))}
              {portfolioPositions.length === 0 && <div className="empty">자동매매 결과가 없습니다.</div>}
            </div>
            <div className="result-total">
              <span>평가손익</span>
              <strong className={portfolioTotals.pnl >= 0 ? 'up' : 'down'}>
                {formatCurrency(portfolioTotals.pnl)} ({formatPct(portfolioTotals.pnl_pct)})
              </strong>
            </div>
          </section>

          <section id="account" className="panel section">
            <div className="section-head">
              <div>
                <h2>계좌 온오프</h2>
                <p>KIS 계좌를 실전 모드로 제어합니다.</p>
              </div>
              <span className="section-meta">{account?.connected ? '연결됨' : '미연결'}</span>
            </div>
            {kisError ? <div className="error-banner">{kisError}</div> : null}
            <div className="kis-list">
              {kisKeys.map((row) => (
                <div key={row.id} className={`kis-row ${row.enabled ? 'on' : 'off'}`}>
                  <div className="kis-main">
                    <div className="kis-label">{row.account || row.id}</div>
                    <div className="kis-desc">{row.description || row.user || '실전 계좌'}</div>
                  </div>
                  <div className="kis-meta">
                    <span>{row.env || 'real'}</span>
                    <span>{row.updated_at ? formatTime(row.updated_at) : '-'}</span>
                  </div>
                  <button className={`kis-toggle ${row.enabled ? 'on' : 'off'}`} onClick={() => handleKisToggle(row)}>
                    {row.enabled ? 'ON' : 'OFF'}
                  </button>
                </div>
              ))}
              {kisKeys.length === 0 && <div className="empty">등록된 계좌가 없습니다.</div>}
            </div>

            <div className="divider" />

            <div className="section-head">
              <div>
                <h2>잔고 · 수익률</h2>
                <p>현재 잔고와 누적 수익률을 확인합니다.</p>
              </div>
              <span className="section-meta">{account?.connected_at ? new Date(account.connected_at).toLocaleDateString('ko-KR') : '-'}</span>
            </div>
            {account?.connected ? (
              <div className="account-metrics">
                <div className="metric-card">
                  <span>총자산</span>
                  <strong>{formatCurrency(accountSummary.total_assets)}</strong>
                </div>
                <div className="metric-card">
                  <span>현금</span>
                  <strong>{formatCurrency(accountSummary.cash)}</strong>
                </div>
                <div className="metric-card">
                  <span>보유자산</span>
                  <strong>{formatCurrency(accountSummary.positions_value)}</strong>
                </div>
                <div className="metric-card">
                  <span>수익률</span>
                  <strong className={accountSummary.total_pnl >= 0 ? 'up' : 'down'}>
                    {formatCurrency(accountSummary.total_pnl)}
                  </strong>
                  <em>{formatPct(accountSummary.total_pnl_pct)}</em>
                </div>
              </div>
            ) : (
              <div className="empty">자동매매 계좌 연결 정보를 불러오지 못했습니다.</div>
            )}
          </section>
        </section>
      </main>

      {selected && modalOpen ? (
        <div className="modal-overlay" onClick={(e) => {
          if (e.target === e.currentTarget) setModalOpen(false)
        }}>
          <div className="modal-panel">
            <div className="modal-head">
              <div>
                <div className="ticker">{selected.code}</div>
                <div className="name">{selected.name}</div>
                <div className="meta">{selected.market} · {selected.sector_name || 'UNKNOWN'}</div>
              </div>
              <div className="modal-actions">
                <div className="range-tabs">
                  {rangeOptions.map((option) => (
                    <button
                      key={option.label}
                      className={days === option.value ? 'active' : ''}
                      onClick={() => setDays(option.value)}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
                <button
                  className={`zoom-toggle ${zoomArmed ? 'on' : ''}`}
                  onClick={() => setZoomArmed((prev) => !prev)}
                >
                  휠 확대 {zoomArmed ? 'ON' : 'OFF'}
                </button>
                <button className="modal-close" onClick={() => setModalOpen(false)}>닫기</button>
              </div>
            </div>

            <div className="chart-grid">
              <div className="chart-summary">
                <div>
                  <div className="ticker">{selected.code}</div>
                  <div className="name">{selected.name}</div>
                  <div className="meta">{selected.market} · {selected.sector_name || 'UNKNOWN'}</div>
                </div>
                <div className={`delta ${delta >= 0 ? 'up' : 'down'}`}>
                  <div className="delta-value">{formatCurrency(latest?.close)}</div>
                  <div className="delta-sub">{formatPct(deltaPct)}</div>
                </div>
              </div>

              <div className="chart-card chart-zoom" ref={chartWheelRef}>
                <div className="chart-title">Price · MA25 · Volume</div>
                {pricesLoading ? (
                  <div className="empty">차트 로딩 중...</div>
                ) : chartData.length === 0 ? (
                  <div className="empty">가격 데이터가 없습니다.</div>
                ) : (
                  <ResponsiveContainer width="100%" height={260}>
                    <ComposedChart data={zoomedData.length ? zoomedData : chartData} margin={{ left: 6, right: 18, top: 10, bottom: 8 }}>
                      <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.08)" />
                      <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#94a3b8' }} interval={Math.max(0, Math.floor((zoomedData.length || chartData.length) / 6))} />
                      <YAxis yAxisId="price" tick={{ fontSize: 11, fill: '#94a3b8' }} domain={['auto', 'auto']} />
                      <YAxis yAxisId="volume" orientation="right" tick={{ fontSize: 11, fill: '#94a3b8' }} />
                      <Tooltip contentStyle={{ background: '#101827', border: '1px solid rgba(148,163,184,0.3)' }} labelStyle={{ color: '#e2e8f0' }} />
                      <Legend wrapperStyle={{ color: '#cbd5f5' }} />
                      <Area yAxisId="price" dataKey="close" stroke="#38bdf8" fill="rgba(14,116,144,0.35)" name="Close" />
                      <Line yAxisId="price" type="monotone" dataKey="ma25" stroke="#f97316" dot={false} strokeWidth={2} name="MA25" />
                      <Bar yAxisId="volume" dataKey="volume" fill="rgba(250,204,21,0.35)" name="Volume" />
                      <Brush
                        dataKey="date"
                        height={24}
                        stroke="#38bdf8"
                        travellerWidth={10}
                        startIndex={zoomRange.start}
                        endIndex={zoomRange.end}
                        onChange={handleBrushChange}
                        data={chartData}
                      />
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
                    <AreaChart data={zoomedData.length ? zoomedData : chartData} margin={{ left: 6, right: 18, top: 10, bottom: 8 }}>
                      <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.08)" />
                      <XAxis dataKey="date" hide />
                      <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} />
                      <Tooltip contentStyle={{ background: '#101827', border: '1px solid rgba(148,163,184,0.3)' }} labelStyle={{ color: '#e2e8f0' }} />
                      <ReferenceLine y={0} stroke="#94a3b8" strokeDasharray="4 4" />
                      <Area dataKey="disparity" stroke="#f43f5e" fill="rgba(248,113,113,0.3)" />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </div>

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
        </div>
      ) : null}
    </div>
  )
}

export default App
