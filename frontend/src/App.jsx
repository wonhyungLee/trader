import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  fetchUniverse,
  fetchPrices,
  fetchCurrentPrice,
  fetchSelection,
  fetchSelectionFilters,
  fetchStatus,
  updateSelectionFilterToggle,
  overrideSector
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

const PRICE_DAYS = 5000
const UNCLASSIFIED_LABEL = '미분류'

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
    return `$${formatNumber(num)}`
  }
  try {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 2
    }).format(num)
  } catch (e) {
    return `$${formatNumber(num)}`
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

const normalizeGroup = (value) => String(value || '').toUpperCase().replace(/&/g, '').replace(/\s+/g, '')

const getGroupTokens = (row) => {
  const raw = normalizeGroup(row?.group ?? row?.group_name ?? '')
  const tokens = raw.split(/[,/|]+/).filter(Boolean)
  return { raw, tokens }
}

const isNasdaqMember = (row) => {
  const { raw, tokens } = getGroupTokens(row)
  if (raw) return raw.includes('NASDAQ100') || tokens.includes('NASDAQ100')
  const market = String(row?.market || '').toUpperCase()
  return market.includes('NASDAQ')
}

const isSp500Member = (row) => {
  const { raw, tokens } = getGroupTokens(row)
  if (raw) return raw.includes('SP500') || tokens.includes('SP500')
  const market = String(row?.market || '').toUpperCase()
  return market.includes('SP500') || market.includes('S&P')
}

function App() {
  const [universe, setUniverse] = useState([])
  const [filter, setFilter] = useState('ALL')
  const [selected, setSelected] = useState(null)
  const [prices, setPrices] = useState([])
  const [pricesLoading, setPricesLoading] = useState(false)
  const [currentPrice, setCurrentPrice] = useState(null)
  const [currentPriceLoading, setCurrentPriceLoading] = useState(false)
  const [selection, setSelection] = useState({ stages: [], candidates: [], stage_items: {} })
  const [status, setStatus] = useState(null)
  const [sectorFilter, setSectorFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [lastUpdated, setLastUpdated] = useState(null)
  const [filterToggles, setFilterToggles] = useState({ min_amount: true, liquidity: true, disparity: true })
  const [filterError, setFilterError] = useState('')
  const [modalOpen, setModalOpen] = useState(false)
  const [zoomRange, setZoomRange] = useState({ start: 0, end: 0 })
  const [zoomArmed, setZoomArmed] = useState(false)
  const [sectorOverrideValue, setSectorOverrideValue] = useState('')
  const [sectorOverrideSaving, setSectorOverrideSaving] = useState(false)
  const [sectorOverrideError, setSectorOverrideError] = useState('')
  const chartWheelRef = useRef(null)
  const [openHelp, setOpenHelp] = useState(null)

  const loadData = () => {
    fetchUniverse().then((data) => setUniverse(asArray(data)))
    fetchSelection().then((data) => {
      const payload = data && typeof data === 'object' ? data : {}
      setSelection({
        ...payload,
        stages: asArray(payload.stages),
        candidates: asArray(payload.candidates),
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
    fetchStatus().then((data) => {
      setStatus(data && typeof data === 'object' ? data : null)
    }).catch(() => {})
    setLastUpdated(new Date())
  }

  useEffect(() => {
    loadData()
    const id = setInterval(() => loadData(), 30000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    if (!selected) return
    setPricesLoading(true)
    fetchPrices(selected.code, PRICE_DAYS)
      .then((data) => {
        setPrices(Array.isArray(data) ? data : [])
      })
      .catch(() => {
        setPrices([])
      })
      .finally(() => setPricesLoading(false))
  }, [selected])

  useEffect(() => {
    if (!selected || !modalOpen) return
    let mounted = true
    const loadCurrentPrice = () => {
      setCurrentPriceLoading(true)
      fetchCurrentPrice(selected.code)
        .then((data) => {
          if (mounted) setCurrentPrice(data && typeof data === 'object' ? data : null)
        })
        .catch(() => {
          if (mounted) setCurrentPrice(null)
        })
        .finally(() => {
          if (mounted) setCurrentPriceLoading(false)
        })
    }
    loadCurrentPrice()
    const id = setInterval(loadCurrentPrice, 60000)
    return () => {
      mounted = false
      clearInterval(id)
    }
  }, [selected?.code, modalOpen])

  useEffect(() => {
    setSectorFilter('ALL')
    setSelected(null)
  }, [filter])

  useEffect(() => {
    setSelected(null)
  }, [sectorFilter])

  useEffect(() => {
    document.body.style.overflow = modalOpen ? 'hidden' : ''
    if (!modalOpen) {
      setCurrentPrice(null)
      setCurrentPriceLoading(false)
      setSectorOverrideValue('')
      setSectorOverrideSaving(false)
      setSectorOverrideError('')
    }
    return () => {
      document.body.style.overflow = ''
    }
  }, [modalOpen])

  useEffect(() => {
    if (!modalOpen) setZoomArmed(false)
  }, [modalOpen])

  const universeRows = useMemo(() => {
    const map = new Map()
    asArray(universe).forEach((row) => {
      if (!row || !row.code) return
      const code = String(row.code).toUpperCase()
      if (!map.has(code)) {
        map.set(code, { ...row, code })
        return
      }
      const existing = map.get(code)
      const mergedGroup = [existing.group, row.group].filter(Boolean).join(',')
      if (mergedGroup) existing.group = mergedGroup
    })
    return Array.from(map.values())
  }, [universe])

  const nasdaqRows = useMemo(() => universeRows.filter(isNasdaqMember), [universeRows])
  const sp500Rows = useMemo(() => universeRows.filter(isSp500Member), [universeRows])
  const allRows = useMemo(() => {
    const map = new Map()
    nasdaqRows.concat(sp500Rows).forEach((row) => {
      if (!map.has(row.code)) map.set(row.code, row)
    })
    return Array.from(map.values())
  }, [nasdaqRows, sp500Rows])

  const activeUniverse = useMemo(() => {
    if (filter === 'NASDAQ100') return nasdaqRows
    if (filter === 'SP500') return sp500Rows
    return allRows
  }, [filter, nasdaqRows, sp500Rows, allRows])

  const universeByCode = useMemo(() => {
    const map = new Map()
    universeRows.forEach((row) => {
      if (!row?.code) return
      map.set(String(row.code).toUpperCase(), row)
    })
    return map
  }, [universeRows])

  const sectorOptions = useMemo(() => {
    const map = new Map()
    activeUniverse.forEach((row) => {
      const name = row.sector_name || UNCLASSIFIED_LABEL
      map.set(name, (map.get(name) || 0) + 1)
    })
    return Array.from(map.entries())
      .map(([sector_name, count]) => ({ sector_name, count }))
      .sort((a, b) => b.count - a.count)
  }, [activeUniverse])

  const knownSectorOptions = useMemo(() => {
    const invalid = new Set(['nan', 'none', 'null', 'na', 'n/a', 'unknown'])
    const map = new Map()
    universeRows.forEach((row) => {
      const raw = String(row?.sector_name || '').trim()
      const name = raw || UNCLASSIFIED_LABEL
      const lower = name.toLowerCase()
      if (!name) return
      if (name === UNCLASSIFIED_LABEL) return
      if (invalid.has(lower)) return
      map.set(name, (map.get(name) || 0) + 1)
    })
    return Array.from(map.entries())
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => (b.count - a.count) || a.name.localeCompare(b.name))
  }, [universeRows])

  const filtered = useMemo(() => {
    const keyword = search.trim().toLowerCase()
    return activeUniverse
      .filter((row) => (sectorFilter === 'ALL' ? true : String(row.sector_name || UNCLASSIFIED_LABEL) === sectorFilter))
      .filter((row) => {
        if (!keyword) return true
        return String(row.code || '').toLowerCase().includes(keyword)
          || String(row.name || '').toLowerCase().includes(keyword)
          || String(row.sector_name || '').toLowerCase().includes(keyword)
      })
  }, [activeUniverse, sectorFilter, search])

  const chartData = useMemo(() => [...prices].reverse(), [prices])
  const latest = chartData.length ? chartData[chartData.length - 1] : null
  const previous = chartData.length > 1 ? chartData[chartData.length - 2] : null
  const delta = latest && previous ? latest.close - previous.close : 0
  const deltaPct = latest && previous && previous.close ? (delta / previous.close) * 100 : 0
  const livePriceValue = currentPrice?.price ?? latest?.close
  const liveChangePct = currentPrice?.change_pct ?? deltaPct
  const liveAsOfLabel = currentPrice?.asof || latest?.date || '-'
  const liveSourceLabel = currentPrice?.source || 'db'

  useEffect(() => {
    if (!chartData.length) return
    setZoomRange({ start: 0, end: chartData.length - 1 })
  }, [chartData.length, modalOpen])

  const selectionStages = asArray(selection?.stages)
  const selectionStageItems = selection?.stage_items && typeof selection.stage_items === 'object' ? selection.stage_items : {}
  const selectionCandidates = asArray(selection?.candidates)

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

  const handleSectorOverrideSave = async () => {
    if (!selected?.code) return
    const sector_name = sectorOverrideValue.trim()
    if (!sector_name) {
      setSectorOverrideError('섹터를 선택하세요.')
      return
    }
    const password = window.prompt('섹터 수정 비밀번호를 입력하세요')
    if (!password) return
    setSectorOverrideSaving(true)
    try {
      const res = await overrideSector(selected.code, sector_name, password)
      setSectorOverrideError('')
      setSelected((prev) => {
        if (!prev) return prev
        return {
          ...prev,
          sector_name: res?.sector_name || sector_name,
          industry_name: res?.industry_name || prev.industry_name,
        }
      })
      loadData()
    } catch (e) {
      const msg = e?.response?.data?.error
      setSectorOverrideError(msg ? `저장 실패: ${msg}` : '섹터 저장에 실패했습니다.')
    } finally {
      setSectorOverrideSaving(false)
    }
  }

  const openStockModal = useCallback((row) => {
    if (!row?.code) return
    const code = String(row.code).toUpperCase()
    const base = universeByCode.get(code) || {}
    setSelected({
      ...base,
      ...row,
      code,
      name: row.name || base.name || code,
      market: row.market || base.market || '-',
      sector_name: row.sector_name || base.sector_name || UNCLASSIFIED_LABEL,
      industry_name: row.industry_name || base.industry_name || '',
    })
    setSectorOverrideValue('')
    setSectorOverrideSaving(false)
    setSectorOverrideError('')
    setModalOpen(true)
  }, [universeByCode])

  const formatStageValue = (stage) => {
    if (!stage) return '-'
    if (stage.key === 'min_amount') return formatCurrency(stage.value)
    if (stage.key === 'liquidity') return `Top ${stage.value}`
    if (stage.key === 'final') return `Max ${stage.value}`
    if (stage.key === 'disparity' && stage.value) {
      const k = formatPct((stage.value.nasdaq || 0) * 100)
      const q = formatPct((stage.value.sp500 || 0) * 100)
      return `NASDAQ ${k} · S&P500 ${q}`
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
  // Final == 최종 후보; avoid showing duplicate cards in the flow grid.
  const flowStages = stageNodes.filter((stage) => stage.key !== 'final')

  const stageColumns = stageNodes.filter((node) => ['min_amount', 'liquidity', 'disparity'].includes(node.key))
  const finalStage = stageNodes.find((node) => node.key === 'final')
  const finalCandidates = useMemo(() => {
    const fromCandidates = selectionCandidates
      .filter((row) => row && row.code)
      .map((row, idx) => ({ ...row, rank: row.rank || (idx + 1) }))
    if (fromCandidates.length) return fromCandidates
    return asArray(finalStage?.items)
      .filter((row) => row && row.code)
      .map((row, idx) => ({ ...row, rank: row.rank || (idx + 1) }))
  }, [selectionCandidates, finalStage])
  const disparityCount = stageMap.disparity?.count || 0
  const finalCount = finalStage?.count || finalCandidates.length
  const finalPassRate = universeCount ? (finalCount / universeCount) * 100 : 0
  const selectionDateLabel = selection?.date || '-'
  const watchdogRuntime = status?.watchdog_runtime && typeof status.watchdog_runtime === 'object'
    ? status.watchdog_runtime
    : {}
  const dailyLockActive = watchdogRuntime.daily_lock_active === true
  const watchdogExternal = status?.watchdog_external && typeof status.watchdog_external === 'object'
    ? status.watchdog_external
    : {}
  const watchdogLastRunTs = Number(watchdogExternal.last_daily_run_ts || 0)
  const watchdogHasState = Object.keys(watchdogExternal).length > 0
  const watchdogMissing = watchdogExternal.last_daily_missing
  const watchdogRunTimeLabel = watchdogLastRunTs
    ? new Date(watchdogLastRunTs * 1000).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' })
    : '-'
  const watchdogDateLabel = watchdogExternal.last_daily_date || '-'
  const watchdogAgeSec = watchdogLastRunTs ? Math.max(0, (Date.now() / 1000) - watchdogLastRunTs) : null
  let watchdogStateText = 'INIT'
  let watchdogStateClass = 'status-warn'
  if (dailyLockActive) {
    watchdogStateText = 'RUNNING'
    watchdogStateClass = 'status-warn'
  } else if (!watchdogHasState) {
    watchdogStateText = 'NO DATA'
  } else if (watchdogExternal.last_daily_rc === 0) {
    if (watchdogAgeSec !== null && watchdogAgeSec > 21600) {
      watchdogStateText = 'STALE'
      watchdogStateClass = 'status-warn'
    } else {
      watchdogStateText = 'OK'
      watchdogStateClass = 'status-ok'
    }
  } else if (watchdogExternal.last_daily_rc != null) {
    watchdogStateText = 'ERROR'
    watchdogStateClass = 'status-error'
  }

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
          <span className="brand-kicker">US MARKET VIEW</span>
          <h1 className="brand-title">BNF US Trade Studio</h1>
          <p className="brand-sub">NASDAQ100 + S&amp;P500 데이터 기반의 시각화 대시보드입니다.</p>
          <p className="brand-note">보유 권장 기간은 6개월입니다.</p>
        </div>
        <div className="controls">
          <div className="segmented">
            <button className={filter === 'ALL' ? 'active' : ''} onClick={() => setFilter('ALL')}>ALL</button>
            <button className={filter === 'NASDAQ100' ? 'active' : ''} onClick={() => setFilter('NASDAQ100')}>NASDAQ 100</button>
            <button className={filter === 'SP500' ? 'active' : ''} onClick={() => setFilter('SP500')}>S&amp;P 500</button>
          </div>
          <div className="control">
            <label>Sector</label>
            <select value={sectorFilter} onChange={e => setSectorFilter(e.target.value)}>
              <option value="ALL">전체 섹터</option>
              {sectorOptions.map((s, i) => (
                <option key={`sector-${s.sector_name}-${i}`} value={s.sector_name}>
                  {s.sector_name} ({s.count})
                </option>
              ))}
            </select>
          </div>
          <a
            className="discord-btn"
            href="https://discord.gg/xHtvSRZG3"
            target="_blank"
            rel="noopener noreferrer"
          >
            디스코드 알람받기
          </a>
          <button className="primary-btn" onClick={() => loadData()}>Refresh</button>
          <div className="refresh-meta">최근 업데이트 {refreshLabel}</div>
        </div>
      </header>

      <section id="summary" className="summary-strip">
        <div className="summary-card">
          <span>유니버스</span>
          <strong>{formatNumber(universeCount)}</strong>
          <em>{filter === 'NASDAQ100' ? 'NASDAQ 100' : 'S&P 500'} 기준</em>
        </div>
        <div className="summary-card">
          <span>필터 통과</span>
          <strong>{formatNumber(disparityCount)}</strong>
          <em>통과율 {universeCount ? ((disparityCount / universeCount) * 100).toFixed(1) : '0.0'}%</em>
        </div>
        <div className="summary-card">
          <span>최종 후보</span>
          <strong>{formatNumber(finalCount)}</strong>
          <em>선정 비율 {universeCount ? finalPassRate.toFixed(1) : '0.0'}%</em>
        </div>
        <div className="summary-card">
          <span>기준일</span>
          <strong>{selectionDateLabel}</strong>
          <em>최근 업데이트 {refreshLabel}</em>
        </div>
        <div className="summary-card">
          <span>DB Watchdog</span>
          <strong className={`status-chip ${watchdogStateClass}`}>{watchdogStateText}</strong>
          <em>{watchdogDateLabel} · {watchdogRunTimeLabel} · miss {watchdogMissing ?? '-'}</em>
        </div>
        <div className="summary-card">
          <span>Discord 알림</span>
          <strong>ON</strong>
          <em>신규 편입 시 전송</em>
        </div>
      </section>

      <nav className="section-nav">
        <a href="#stocks">주식목록</a>
        <a href="#filters">선별 과정</a>
        <a href="#final">최종 후보</a>
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
                onClick={() => openStockModal(row)}
              >
                <div>
                  <div className="ticker">{row.code}</div>
                  <div className="name">{row.name}</div>
                  <div className="meta">
                    <span>{row.sector_name || UNCLASSIFIED_LABEL}</span>
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
              {flowStages.map((stage) => (
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
              <div id="final" className="final-board">
                <div className="final-head">
                  <div>
                    <div className="filter-tag">최종 후보</div>
                    <div className="filter-title-row">
                      <div className="final-title">최종 후보</div>
                      <button
                        type="button"
                        className="help-icon"
                        aria-label="최종 후보 설명"
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
                  <div className="filter-count">{finalCount}</div>
                </div>
                <div className="result-table">
                  <div className="result-row head">
                    <span>Rank</span>
                    <span>Code</span>
                    <span>Name</span>
                    <span>Amount</span>
                    <span>Disparity</span>
                    <span>Market</span>
                  </div>
                  {finalCandidates.slice(0, 25).map((row, idx) => (
                    <div
                      key={`final-${row.code}-${idx}`}
                      className="result-row result-clickable"
                      role="button"
                      tabIndex={0}
                      onClick={() => openStockModal(row)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault()
                          openStockModal(row)
                        }
                      }}
                    >
                      <span className="mono">{row.rank || '-'}</span>
                      <span className="mono">{row.code}</span>
                      <span>{row.name || '-'}</span>
                      <span>{formatCurrency(row.amount)}</span>
                      <span className={(row.disparity ?? 0) <= 0 ? 'down' : 'up'}>
                        {formatPct((row.disparity || 0) * 100)}
                      </span>
                      <span>{row.market || '-'}</span>
                    </div>
                  ))}
                  {finalCandidates.length === 0 && <div className="empty">최종 후보가 없습니다.</div>}
                </div>
              </div>
            ) : null}
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
                <div className="meta">{selected.market} · {selected.sector_name || UNCLASSIFIED_LABEL}</div>
                {(selected.sector_name || UNCLASSIFIED_LABEL) === UNCLASSIFIED_LABEL ? (
                  <div className="sector-override">
                    <span className="sector-override-label">섹터 지정</span>
                    <select
                      value={sectorOverrideValue}
                      onChange={(e) => {
                        setSectorOverrideValue(e.target.value)
                        setSectorOverrideError('')
                      }}
                      disabled={sectorOverrideSaving || knownSectorOptions.length === 0}
                    >
                      <option value="">기존 섹터 선택...</option>
                      {knownSectorOptions.map((opt) => (
                        <option key={opt.name} value={opt.name}>{opt.name} ({opt.count})</option>
                      ))}
                    </select>
                    <button
                      className="sector-override-btn"
                      onClick={handleSectorOverrideSave}
                      disabled={!sectorOverrideValue || sectorOverrideSaving}
                    >
                      {sectorOverrideSaving ? '저장 중...' : '저장'}
                    </button>
                    {sectorOverrideError ? (
                      <span className="sector-override-error">{sectorOverrideError}</span>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="modal-actions">
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
                  <div className="meta">{selected.market} · {selected.sector_name || UNCLASSIFIED_LABEL}</div>
                </div>
                <div className={`delta ${Number(liveChangePct || 0) >= 0 ? 'up' : 'down'}`}>
                  <div className="delta-label">Current Price {currentPriceLoading ? '· 업데이트 중' : ''}</div>
                  <div className="delta-value">{formatCurrency(livePriceValue)}</div>
                  <div className="delta-sub">{formatPct(liveChangePct)} · {formatTime(liveAsOfLabel)} · {String(liveSourceLabel).toUpperCase()}</div>
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
