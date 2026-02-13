import { useEffect, useMemo, useRef, useState } from 'react'
import { fetchCoupangBanner } from './api'

const STORAGE_KEY_NEXT_TS = 'cp_banner_next_ts'
const COOLDOWN_MS = 6 * 60 * 60 * 1000

const clampInt = (value, fallback) => {
  const num = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(num) ? num : fallback
}

const formatCurrencyKRW = (value) => {
  const text = String(value ?? '').trim()
  if (!text) return ''
  if (text.endsWith('원')) return text
  const num = Number(text.replace(/,/g, ''))
  if (!Number.isFinite(num)) return text
  return `${num.toLocaleString('ko-KR')}원`
}

export default function CoupangBanner({ disabled = false }) {
  const [visible, setVisible] = useState(false)
  const [item, setItem] = useState(null)
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(false)
  const timerRef = useRef(null)
  const actionRef = useRef(false)

  const now = () => Date.now()

  const readNextTs = () => {
    if (typeof localStorage === 'undefined') return 0
    return clampInt(localStorage.getItem(STORAGE_KEY_NEXT_TS), 0)
  }

  const writeNextTs = (ts) => {
    if (typeof localStorage === 'undefined') return
    localStorage.setItem(STORAGE_KEY_NEXT_TS, String(ts))
  }

  const scheduleNext = (nextTs) => {
    if (timerRef.current) {
      clearTimeout(timerRef.current)
      timerRef.current = null
    }
    if (!nextTs) return
    const delay = Math.max(1000, nextTs - now())
    timerRef.current = setTimeout(() => {
      timerRef.current = null
      if (!disabled) {
        load()
      }
    }, delay)
  }

  const setCooldown = () => {
    const nextTs = now() + COOLDOWN_MS
    writeNextTs(nextTs)
    scheduleNext(nextTs)
    setVisible(false)
  }

  const openAndCooldown = () => {
    if (actionRef.current) return
    actionRef.current = true
    try {
      setCooldown()
      if (link) {
        const opened = window.open(link, '_blank', 'noopener,noreferrer')
        if (!opened) window.location.href = link
      }
    } finally {
      // If navigation is blocked, allow retry.
      setTimeout(() => {
        actionRef.current = false
      }, 800)
    }
  }

  const load = async () => {
    if (disabled) return
    const nextTs = readNextTs()
    const remaining = nextTs - now()
    if (remaining > 0) {
      setVisible(false)
      scheduleNext(nextTs)
      return
    }

    setLoading(true)
    try {
      const payload = await fetchCoupangBanner({ limit: 1 })
      const first = payload?.items?.[0]
      if (!first?.link) {
        setVisible(false)
        setItem(null)
        setMeta(payload?.theme || null)
        return
      }
      setMeta(payload?.theme || null)
      setItem(first)
      setVisible(true)
    } catch {
      setVisible(false)
      setItem(null)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [disabled])

  useEffect(() => {
    if (!visible) return
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.body.style.overflow = prev
    }
  }, [visible])

  const title = String(item?.title || '').trim()
  const price = formatCurrencyKRW(item?.price)
  const image = String(item?.image || '').trim()
  const link = String(item?.link || '').trim()
  const badge = String(item?.badge || meta?.title || '추천').trim()
  const cta = String(item?.cta || meta?.cta || '보기').trim()
  const metaLine = String(item?.meta || meta?.tagline || '').trim()

  const isReady = useMemo(() => visible && !!link && !disabled, [visible, link, disabled])
  if (!isReady) return null

  return (
    <div className="cp-pop" role="dialog" aria-modal="true" aria-label="쿠팡 추천 광고">
      <div className="cp-pop-backdrop" onClick={openAndCooldown} />
      <div
        className={`cp-pop-card ${loading ? 'loading' : ''}`}
        role="button"
        tabIndex={0}
        onClick={openAndCooldown}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            openAndCooldown()
          }
        }}
      >
        <button
          type="button"
          className="cp-pop-close"
          onClick={(e) => {
            e.preventDefault()
            e.stopPropagation()
            openAndCooldown()
          }}
          aria-label="광고 열기"
          title="광고 열기"
        >
          ×
        </button>

        <div className="cp-pop-head">
          <span className="cp-pop-badge">{badge}</span>
          <span className="cp-pop-tagline">{String(meta?.tagline || '오늘 필요한 생필품 추천').trim()}</span>
        </div>

        <div className="cp-pop-body">
          {image ? <img className="cp-pop-img" src={image} alt={title || '추천 상품'} loading="lazy" /> : null}
          <div className="cp-pop-copy">
            <div className="cp-pop-title">{title || '추천 상품'}</div>
            {price ? <div className="cp-pop-price">{price}</div> : null}
            {metaLine ? <div className="cp-pop-meta">{metaLine}</div> : null}
            <div className="cp-pop-cta">{cta}</div>
            <div className="cp-pop-disclosure">쿠팡파트너스 활동으로 수수료를 제공받을 수 있습니다.</div>
          </div>
        </div>
      </div>
    </div>
  )
}
