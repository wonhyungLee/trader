import { useEffect, useMemo, useState } from 'react'
import { fetchUniverse, fetchPrices, fetchSignals } from './api'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import './App.css'

function App() {
  const [universe, setUniverse] = useState([])
  const [filter, setFilter] = useState('KOSPI100')
  const [selected, setSelected] = useState(null)
  const [prices, setPrices] = useState([])
  const [days, setDays] = useState(60)
  const [signals, setSignals] = useState([])

  useEffect(() => {
    fetchUniverse().then(setUniverse)
    fetchSignals().then(setSignals)
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
        </div>
      </main>
    </div>
  )
}

export default App
