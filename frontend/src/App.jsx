import { useEffect, useMemo, useState } from 'react'
import { fetchUniverse, fetchPrices, fetchSignals, fetchStatus, fetchEngines, fetchOrders, fetchPositions, fetchStrategy, fetchJobs } from './api'
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
  const [jobs, setJobs] = useState([])

  const loadData = () => {
    fetchUniverse().then(setUniverse)
    fetchSignals().then(setSignals)
    fetchStatus().then(setStatus)
    fetchEngines().then(setEngines)
    fetchOrders().then(setOrders)
    fetchPositions().then(setPositions)
    fetchStrategy().then(setStrategy)
    fetchJobs().then(setJobs)
  }

  useEffect(() => {
    loadData()
    const id = setInterval(loadData, 30000)
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
          <button onClick={loadData} className="refresh-btn">새로고침</button>
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
          <div className="dashboard-grid">
            {engines && (
              <div className="panel status-panel">
                <div className="panel-title">엔진 상태</div>
                <div className="status-grid">
                  <div className={`status-card ${engines.monitor?.running ? 'on' : 'off'}`}>
                    <label>모니터링</label>
                    <div className="value">{engines.monitor?.running ? 'RUNNING' : 'STOPPED'}</div>
                  </div>
                  <div className="status-card on">
                    <label>자동매매(트레이더)</label>
                    <div className="value">P:{engines.trader?.pending} S:{engines.trader?.sent} D:{engines.trader?.done}</div>
                  </div>
                  <div className={`status-card ${engines.accuracy_loader?.running ? 'on' : 'off'}`}>
                    <label>정확도 수집</label>
                    <div className="value">{engines.accuracy_loader?.running ? 'RUNNING' : 'STOPPED'}</div>
                  </div>
                </div>
              </div>
            )}

            {status && (
              <div className="panel status-panel">
                <div className="panel-title">데이터 준비도</div>
                <div className="status-grid">
                  <div className="status-card on">
                    <label>유니버스 (250)</label>
                    <div className="value">{status.universe?.total} 종목</div>
                  </div>
                  <div className="status-card on">
                    <label>일봉 데이터</label>
                    <div className="value">{status.daily_price?.codes} / 250 (결측:{status.daily_price?.missing_codes})</div>
                    <div className="sub-value">{status.daily_price?.date?.min} ~ {status.daily_price?.date?.max}</div>
                  </div>
                  <div className="status-card on">
                    <label>정확도 결측</label>
                    <div className="value">INV:{status.accuracy?.investor_flow_daily?.missing_codes} SHT:{status.accuracy?.short_sale_daily?.missing_codes}</div>
                  </div>
                </div>
              </div>
            )}
          </div>

          <div className="panel jobs-panel">
            <div className="panel-title">최근 작업 로그</div>
            <div className="t-compact">
              <div className="t-head">job_name</div>
              <div className="t-head">started</div>
              <div className="t-head">finished</div>
              <div className="t-head">status</div>
              <div className="t-head">message</div>
              {jobs.map((j, i) => (
                <div key={i} className="t-row">
                  <div className="b">{j.job_name}</div>
                  <div>{j.started_at?.split('T')[1].split('.')[0]}</div>
                  <div>{j.finished_at ? j.finished_at.split('T')[1].split('.')[0] : '-'}</div>
                  <div className={j.status === 'SUCCESS' ? 'text-ok' : 'text-err'}>{j.status}</div>
                  <div className="ellipsis">{j.message}</div>
                </div>
              ))}
            </div>
          </div>

          {selected ? (
            <>
              <h3>{selected.code} {selected.name}</h3>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={[...prices].reverse()} margin={{left:12,right:12,top:12,bottom:12}}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" tick={{fontSize:11}} interval={Math.max(0, Math.floor(prices.length/8))} />
                  <YAxis tick={{fontSize:11}} domain={['auto','auto']} />
                  <Tooltip />
                  <Line type="monotone" dataKey="close" stroke="#2563eb" dot={false} strokeWidth={2} />
                  <Line type="monotone" dataKey="ma25" stroke="#f97316" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </>
          ) : (
            <div className="placeholder">왼쪽에서 종목을 선택하세요.</div>
          )}

          <div className="panel positions-panel">
            <div className="panel-title">보유 포지션</div>
            <div className="t-compact">
              <div className="t-head">code</div>
              <div className="t-head">name</div>
              <div className="t-head">qty</div>
              <div className="t-head">avg_price</div>
              <div className="t-head">entry_date</div>
              <div className="t-head">updated</div>
              {positions.length === 0 && <div className="t-row"><div style={{gridColumn:'span 6', textAlign:'center', padding:'20px', color:'#94a3b8'}}>보유 포지션이 없습니다.</div></div>}
              {positions.map((p, i) => (
                <div key={i} className="t-row">
                  <div className="b">{p.code}</div>
                  <div>{p.name}</div>
                  <div className="b">{p.qty}</div>
                  <div>{p.avg_price?.toLocaleString()}</div>
                  <div>{p.entry_date}</div>
                  <div>{p.updated_at?.split('T')[1].split('.')[0]}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="panel orders-panel">
            <div className="panel-title">최근 주문 내역</div>
            <div className="t-compact">
              <div className="t-head">exec_date</div>
              <div className="t-head">code</div>
              <div className="t-head">side</div>
              <div className="t-head">qty</div>
              <div className="t-head">status</div>
              <div className="t-head">avg_price</div>
              {orders.map((o, i) => (
                <div key={i} className="t-row">
                  <div>{o.exec_date}</div>
                  <div className="b">{o.code}</div>
                  <div className={o.side === 'BUY' ? 'text-buy' : 'text-sell'}>{o.side}</div>
                  <div className="b">{o.qty}</div>
                  <div className={`status-tag ${o.status}`}>{o.status}</div>
                  <div>{o.avg_price?.toLocaleString()}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="panel strategy-panel">
            <div className="panel-title">전략 파라미터</div>
            {strategy && (
              <div className="kv-grid">
                <div className="kv"><span>괴리율(KOSPI)</span><b>{strategy.disparity_buy_kospi}</b></div>
                <div className="kv"><span>괴리율(KOSDAQ)</span><b>{strategy.disparity_buy_kosdaq}</b></div>
                <div className="kv"><span>익절괴리율</span><b>{strategy.disparity_sell}</b></div>
                <div className="kv"><span>손절률</span><b>{strategy.stop_loss}</b></div>
                <div className="kv"><span>최대보유일</span><b>{strategy.max_holding_days}</b></div>
                <div className="kv"><span>주문금액</span><b>{strategy.order_value?.toLocaleString()}</b></div>
                <div className="kv"><span>주문구분</span><b>{strategy.ord_dvsn === '01' ? '시장가' : '지정가'}</b></div>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  )
}

export default App