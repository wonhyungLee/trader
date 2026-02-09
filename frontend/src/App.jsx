import { useEffect, useMemo, useState } from 'react'
import { fetchUniverse, fetchPrices, fetchSignals, fetchStatus, fetchEngines, fetchOrders, fetchPositions, fetchStrategy } from './api'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import './App.css'

function App() {
  const [universe, setUniverse] = useState([])
  const [filter, setFilter] = useState('KOSPI100')
  const [selected, setSelected] = useState(null)
  const [prices, setPrices] = useState([])
  const [days, setDays] = useState(60)
  const [signals, setSignals] = useState([])
  const [status, setStatus] = useState(null)
  const [engines, setEngines] = useState(null)
  const [orders, setOrders] = useState([])
  const [positions, setPositions] = useState([])
  const [strategy, setStrategy] = useState(null)

  useEffect(() => {
    fetchUniverse().then(setUniverse)
    fetchSignals().then(setSignals)
    fetchStatus().then(setStatus)
    fetchEngines().then(setEngines)
    fetchOrders().then(setOrders)
    fetchPositions().then(setPositions)
    fetchStrategy().then(setStrategy)
  }, [])

  useEffect(() => {
    const id = setInterval(() => {
      fetchSignals().then(setSignals)
      fetchStatus().then(setStatus)
      fetchEngines().then(setEngines)
      fetchOrders().then(setOrders)
      fetchPositions().then(setPositions)
    }, 30000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    if (selected) {
      fetchPrices(selected.code, days).then(setPrices)
    }
  }, [selected, days])

  const filtered = useMemo(() => universe.filter(u => u.group === filter), [universe, filter])

  return (
    <div className="page">
      <header>
        <h1>BNF-K 모니터링</h1>
        <div className="filters">
          <select value={filter} onChange={e => setFilter(e.target.value)}>
            <option value="KOSPI100">KOSPI 100</option>
            <option value="KOSDAQ150">KOSDAQ 150</option>
          </select>
          <input type="number" value={days} onChange={e => setDays(Number(e.target.value)||30)} min={10} max={400} />
        </div>
      </header>

      <main className="layout">
        <div className="list">
          <div className="list-header">총 {filtered.length} 종목</div>
          <div className="table">
            {filtered.map((row) => (
              <div key={row.code} className={selected?.code === row.code ? 'row active' : 'row'} onClick={() => setSelected(row)}>
                <div className="code">{row.code}</div>
                <div className="name">{row.name}</div>
                <div className="mkt">{row.market}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="chart">
          {engines && (
            <div className="status">
              <div className="status-title">엔진 상태</div>
              <div className="status-grid">
                <div className="status-card">
                  <div className="label">종목 선별 엔진</div>
                  <div className={engines.monitor?.running ? 'value ok' : 'value'}>
                    {engines.monitor?.running ? 'RUNNING' : 'STOPPED'}
                  </div>
                </div>
                <div className="status-card">
                  <div className="label">자동매매 엔진</div>
                  <div className="value">last_signal: {engines.trader?.last_signal || '-'}</div>
                  <div className="value">pending: {engines.trader?.pending ?? 0} / sent: {engines.trader?.sent ?? 0} / done: {engines.trader?.done ?? 0}</div>
                </div>
                <div className="status-card">
                  <div className="label">정확도 수집</div>
                  <div className={engines.accuracy_loader?.running ? 'value ok' : 'value'}>
                    {engines.accuracy_loader?.running ? `RUNNING (pid ${engines.accuracy_loader?.pid || '-'})` : 'STOPPED'}
                  </div>
                  <div className="value">progress: {engines.accuracy_loader?.progress?.last_index ?? '-'} / {engines.accuracy_loader?.progress?.total ?? '-'}</div>
                </div>
              </div>
            </div>
          )}

          {status && (
            <div className="status">
              <div className="status-title">데이터 상태</div>
              <div className="status-grid">
                <div className="status-card">
                  <div className="label">stock_info</div>
                  <div className="value">rows: {status.stock_info?.rows ?? 0}</div>
                </div>
                <div className="status-card">
                  <div className="label">daily_price</div>
                  <div className="value">rows: {status.daily_price?.rows ?? 0}</div>
                  <div className="value">codes: {status.daily_price?.codes ?? 0}</div>
                  <div className="value">range: {status.daily_price?.date?.min || '-'} ~ {status.daily_price?.date?.max || '-'}</div>
                </div>
                <div className="status-card">
                  <div className="label">accuracy (missing)</div>
                  <div className="value">investor: {status.accuracy?.investor_flow_daily?.missing_codes ?? 0}</div>
                  <div className="value">program: {status.accuracy?.program_trade_daily?.missing_codes ?? 0}</div>
                  <div className="value">short: {status.accuracy?.short_sale_daily?.missing_codes ?? 0}</div>
                  <div className="value">credit: {status.accuracy?.credit_balance_daily?.missing_codes ?? 0}</div>
                  <div className="value">loan: {status.accuracy?.loan_trans_daily?.missing_codes ?? 0}</div>
                  <div className="value">vi: {status.accuracy?.vi_status_daily?.missing_codes ?? 0}</div>
                </div>
              </div>
            </div>
          )}

          {selected ? (
            <>
              <h3>{selected.code} {selected.name}</h3>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={[...prices].reverse()} margin={{left:12,right:12,top:12,bottom:24}}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" tick={{fontSize:11}} interval={Math.max(0, Math.floor(prices.length/8))} />
                  <YAxis tick={{fontSize:11}} domain={['auto','auto']} />
                  <Tooltip />
                  <Line type="monotone" dataKey="close" stroke="#2563eb" dot={false} />
                  <Line type="monotone" dataKey="ma25" stroke="#f97316" dot={false} />
                </LineChart>
              </ResponsiveContainer>
              <div className="price-table">
                <div className="price-table-title">가격 테이블 (최근 {days}일)</div>
                <div className="table-grid">
                  <div className="table-head">date</div>
                  <div className="table-head">open</div>
                  <div className="table-head">high</div>
                  <div className="table-head">low</div>
                  <div className="table-head">close</div>
                  <div className="table-head">volume</div>
                  <div className="table-head">amount</div>
                  {prices.map((p, i) => (
                    <div key={i} className="table-row">
                      <div>{p.date}</div>
                      <div>{p.open}</div>
                      <div>{p.high}</div>
                      <div>{p.low}</div>
                      <div>{p.close}</div>
                      <div>{p.volume}</div>
                      <div>{p.amount}</div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          ) : (
            <div className="placeholder">왼쪽에서 종목을 선택하세요.</div>
          )}

          <div className="signals">
            <div className="signals-header">최근 신호</div>
            <div className="signals-body">
              {signals.map((s, i) => (
                <div key={i} className="signal-row">
                  <span>{s.signal_date}</span>
                  <span>{s.code}</span>
                  <span>{s.side}</span>
                  <span>{s.qty}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="panel">
            <div className="panel-title">전략 파라미터</div>
            {strategy ? (
              <div className="kv-grid">
                <div className="kv"><span>liquidity_rank</span><b>{strategy.liquidity_rank}</b></div>
                <div className="kv"><span>min_amount</span><b>{strategy.min_amount}</b></div>
                <div className="kv"><span>buy_kospi</span><b>{strategy.disparity_buy_kospi}</b></div>
                <div className="kv"><span>buy_kosdaq</span><b>{strategy.disparity_buy_kosdaq}</b></div>
                <div className="kv"><span>sell_disparity</span><b>{strategy.disparity_sell}</b></div>
                <div className="kv"><span>stop_loss</span><b>{strategy.stop_loss}</b></div>
                <div className="kv"><span>max_holding_days</span><b>{strategy.max_holding_days}</b></div>
                <div className="kv"><span>order_value</span><b>{strategy.order_value}</b></div>
                <div className="kv"><span>ord_dvsn</span><b>{strategy.ord_dvsn}</b></div>
              </div>
            ) : (
              <div className="placeholder">전략 파라미터 로딩 중...</div>
            )}
          </div>

          <div className="panel">
            <div className="panel-title">보유 포지션</div>
            <div className="table-compact">
              <div className="t-head">code</div>
              <div className="t-head">name</div>
              <div className="t-head">qty</div>
              <div className="t-head">avg_price</div>
              <div className="t-head">stop_loss</div>
              <div className="t-head">entry_date</div>
              <div className="t-head">updated_at</div>
              {positions.map((p, i) => (
                <div key={i} className="t-row">
                  <div>{p.code}</div>
                  <div>{p.name}</div>
                  <div>{p.qty}</div>
                  <div>{p.avg_price}</div>
                  <div>{strategy?.stop_loss != null ? Math.round(p.avg_price * (1 + strategy.stop_loss)) : '-'}</div>
                  <div>{p.entry_date}</div>
                  <div>{p.updated_at}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="panel">
            <div className="panel-title">주문/체결 내역</div>
            <div className="table-compact">
              <div className="t-head">signal</div>
              <div className="t-head">exec</div>
              <div className="t-head">code</div>
              <div className="t-head">side</div>
              <div className="t-head">qty</div>
              <div className="t-head">status</div>
              <div className="t-head">ord_unpr</div>
              <div className="t-head">avg_price</div>
              {orders.map((o, i) => (
                <div key={i} className="t-row">
                  <div>{o.signal_date}</div>
                  <div>{o.exec_date}</div>
                  <div>{o.code}</div>
                  <div>{o.side}</div>
                  <div>{o.qty}</div>
                  <div>{o.status}</div>
                  <div>{o.ord_unpr}</div>
                  <div>{o.avg_price}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}

export default App
