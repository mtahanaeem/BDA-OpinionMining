import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Menu, HelpCircle, Bell, Database, Activity, Cpu, AlertTriangle,
  BarChart3, Layers, ChevronDown, ChevronRight, Zap, Globe,
  TrendingUp, PieChart, Radio, Hash, Clock, Shield
} from 'lucide-react'
import './Dashboard.css'

/* ═══════════════════════════════════════════ */
/*  Constants                                  */
/* ═══════════════════════════════════════════ */

const SAMPLE_TEXTS = [
  "This product is absolutely amazing! Totally recommend it to everyone.",
  "Terrible customer service, very disappointed with the response time.",
  "It's okay, nothing special but gets the job done I suppose.",
  "Love the new update! The interface is so much cleaner now.",
  "Worst experience ever. Complete waste of money and time.",
  "Pretty decent quality for the price point. Would buy again.",
  "The team did an incredible job on this release. Kudos!",
  "Not sure what all the hype is about. It's mediocre at best.",
  "Absolutely fantastic! Exceeded all my expectations by far.",
  "This is broken beyond belief. Fix your product please.",
  "Brilliant innovation! This changes everything.",
  "Really frustrated with the constant bugs and crashes.",
  "Solid performance overall, minor issues here and there.",
  "Couldn't be happier with my purchase! Five stars.",
  "The battery life is unacceptable for the price paid.",
  "Works as expected, no complaints whatsoever.",
  "Game changer! Industry leading quality and support.",
  "Overpriced and underwhelming. Not worth it at all.",
  "Highly recommend this to anyone in the market.",
  "Very poor build quality. Falling apart already.",
  "Average at best. Nothing groundbreaking here.",
  "Outstanding! Best purchase I've made this year.",
]

const POS_KW = ['amazing','love','fantastic','brilliant','perfect','outstanding',
  'incredible','happier','exceeded','game changer','kudos','best','recommend']
const NEG_KW = ['terrible','worst','broken','frustrated','unacceptable',
  'underwhelming','overpriced','poor','waste','disappointed','crashes','fix']

function inferSentiment(text) {
  const l = text.toLowerCase()
  const p = POS_KW.some(w => l.includes(w))
  const n = NEG_KW.some(w => l.includes(w))
  if (p && !n) return 'POSITIVE'
  if (n && !p) return 'NEGATIVE'
  if (p && n) return Math.random() > 0.5 ? 'POSITIVE' : 'NEGATIVE'
  return 'NEUTRAL'
}

const SOURCES = [
  { key: 'twitter', label: 'Twitter/X', color: '#22d3ee', weight: 40 },
  { key: 'reddit', label: 'Reddit', color: '#a78bfa', weight: 28 },
  { key: 'news', label: 'News', color: '#34d399', weight: 18 },
  { key: 'other', label: 'Other', color: '#64748b', weight: 14 },
]

function pickSource() {
  const r = Math.random() * 100
  let cum = 0
  for (const s of SOURCES) { cum += s.weight; if (r <= cum) return s.key }
  return 'other'
}

function genConf(s) { return Math.round(((s === 'NEUTRAL' ? 78 : 85) + Math.random() * 14) * 10) / 10 }
const MAX_LOG = 7

/* ═══════════════════════════════════════════ */
/*  SVG Helpers                                */
/* ═══════════════════════════════════════════ */

function smoothPath(data, x0, y0, sx, sy) {
  if (data.length < 2) return ''
  const pts = data.map((v, i) => ({ x: x0 + i * sx, y: y0 + (100 - v) * sy }))
  let d = `M ${pts[0].x} ${pts[0].y}`
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(0, i - 1)], p1 = pts[i], p2 = pts[i + 1]
    const p3 = pts[Math.min(pts.length - 1, i + 2)]
    const c1x = p1.x + (p2.x - p0.x) / 6, c1y = p1.y + (p2.y - p0.y) / 6
    const c2x = p2.x - (p3.x - p1.x) / 6, c2y = p2.y - (p3.y - p1.y) / 6
    d += ` C ${c1x} ${c1y}, ${c2x} ${c2y}, ${p2.x} ${p2.y}`
  }
  return d
}

function fillArea(data, x0, y0, sx, sy, by) {
  const l = smoothPath(data, x0, y0, sx, sy)
  if (!l) return ''
  return `${l} L ${x0 + (data.length - 1) * sx} ${by} L ${x0} ${by} Z`
}

function fmt(n) { return n.toLocaleString('en-US') }

/* ═══════════════════════════════════════════ */
/*  Sub-components                             */
/* ═══════════════════════════════════════════ */

function Donut({ size = 140, sw = 24, segs, label }) {
  const r = (size - sw) / 2, circ = 2 * Math.PI * r, cx = size / 2
  let off = 0
  return (
    <div className="flex flex-col items-center gap-2">
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle cx={cx} cy={cx} r={r} fill="none" stroke="rgba(148,163,184,0.08)" strokeWidth={sw} />
        {segs.map((s, i) => {
          const len = Math.max((s.v / 100) * circ, 0.5), dash = `${len} ${circ}`, o = off
          off += len
          return <circle key={i} cx={cx} cy={cx} r={r} fill="none" stroke={s.c} strokeWidth={sw}
            strokeDasharray={dash} strokeDashoffset={-o} transform={`rotate(-90 ${cx} ${cx})`}
            strokeLinecap="round" style={{ transition: 'stroke-dashoffset 0.5s ease' }} />
        })}
      </svg>
      <span className="text-[10px] text-slate-400 font-medium tracking-wide">{label}</span>
    </div>
  )
}

function SentBadge({ label }) {
  const m = { POSITIVE: 'badge-positive', NEGATIVE: 'badge-negative', NEUTRAL: 'badge-neutral' }
  return <span className={`sentiment-badge ${m[label] || m.NEUTRAL}`}>{label}</span>
}

/* ═══════════════════════════════════════════ */
/*  Main Dashboard                             */
/* ═══════════════════════════════════════════ */

export default function Dashboard() {
  /* State */
  const [totalTweets, setTotalTweets] = useState(81730)
  const [rate, setRate] = useState(4.7)
  const [avgConf, setAvgConf] = useState(92.4)
  const [confN, setConfN] = useState(5000)
  const [sc, setSc] = useState({ pos: 6138, neg: 13287, neu: 51070 })
  const [srcC, setSrcC] = useState({ twitter: 44, reddit: 28, news: 18, other: 10 })
  const [logs, setLogs] = useState([])
  const [feedKeys, setFeedKeys] = useState(new Set())
  const [pm, setPm] = useState({ spark: 70424, kafka: 9542, hf: 1764 })
  const [expanded, setExpanded] = useState(null)
  const [timelineTab, setTimelineTab] = useState('Month')
  const tickRef = useRef(0)

  /* Rolling trend buffers */
  const T = 28
  const [tp, setTp] = useState(() => Array(T).fill(22))
  const [tn, setTn] = useState(() => Array(T).fill(50))
  const [tng, setTng] = useState(() => Array(T).fill(18))

  /* Generate tweet */
  const genTweet = useCallback(() => {
    const text = SAMPLE_TEXTS[Math.floor(Math.random() * SAMPLE_TEXTS.length)]
    const sentiment = inferSentiment(text)
    return {
      id: `tw_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
      text, sentiment, confidence: genConf(sentiment),
      source: pickSource(), timestamp: new Date().toISOString(),
    }
  }, [])

  /* Tick */
  const handleTick = useCallback(() => {
    const batch = 1 + Math.floor(Math.random() * 3)
    const tweets = Array.from({ length: batch }, () => genTweet())
    let dp = 0, dn = 0, dneu = 0, cs = 0
    const sd = { twitter: 0, reddit: 0, news: 0, other: 0 }
    const rows = []

    tweets.forEach(t => {
      if (t.sentiment === 'POSITIVE') dp++; else if (t.sentiment === 'NEGATIVE') dn++; else dneu++
      sd[t.source]++
      cs += t.confidence
      rows.push({ ...t, key: `r_${Date.now()}_${Math.random().toString(36).slice(2)}` })
    })

    setTotalTweets(p => p + batch)
    setSc(p => ({ pos: p.pos + dp, neg: p.neg + dn, neu: p.neu + dneu }))
    setSrcC(p => ({ twitter: p.twitter + sd.twitter, reddit: p.reddit + sd.reddit, news: p.news + sd.news, other: p.other + sd.other }))
    setConfN(p => p + batch)
    setAvgConf(p => Math.round((p * confN + cs) / (confN + batch) * 10) / 10)
    setRate(() => Math.round((3.0 + Math.random() * 3.2) * 10) / 10)
    setPm(p => ({ spark: p.spark + 10 + Math.floor(Math.random() * 30), kafka: p.kafka + 5 + Math.floor(Math.random() * 20), hf: p.hf + 2 + Math.floor(Math.random() * 8) }))
    setLogs(p => { const n = [...rows, ...p]; return n.slice(0, MAX_LOG) })

    const ks = new Set(rows.map(r => r.key))
    setFeedKeys(ks)
    setTimeout(() => setFeedKeys(new Set()), 800)

    const t = tickRef.current + 1
    tickRef.current = t
    if (t % 3 === 0) {
      const total = dp + dn + dneu || 1
      setTp(p => [...p.slice(1), (dp / total) * 100])
      setTn(p => [...p.slice(1), (dneu / total) * 100])
      setTng(p => [...p.slice(1), (dn / total) * 100])
    }
  }, [genTweet, confN])

  /* Interval */
  useEffect(() => {
    const id = setInterval(handleTick, 700 + Math.random() * 500)
    return () => clearInterval(id)
  }, [handleTick])

  /* Derived */
  const sTot = sc.pos + sc.neg + sc.neu || 1
  const sp = { pos: (sc.pos / sTot) * 100, neg: (sc.neg / sTot) * 100, neu: (sc.neu / sTot) * 100 }
  const srTot = srcC.twitter + srcC.reddit + srcC.news + srcC.other || 1
  const sr = { twitter: (srcC.twitter / srTot) * 100, reddit: (srcC.reddit / srTot) * 100, news: (srcC.news / srTot) * 100, other: (srcC.other / srTot) * 100 }

  /* ─── Timeline windowing ─── */
  const windowSize = timelineTab === 'Day' ? 7 : timelineTab === 'Week' ? 14 : 28
  const twp = tp.slice(-windowSize)
  const twn = tn.slice(-windowSize)
  const twng = tng.slice(-windowSize)

  /* Chart dims */
  const W = 500, H = 150, PL = 28, PR = 8, PT = 6, PB = 18
  const cw = W - PL - PR, ch = H - PT - PB, sx = cw / (twp.length - 1 || 1)
  const xTicks = twp.map((_, i) => ({ label: `D${i + 1}`, x: PL + i * sx })).filter((_, i) => i % Math.max(1, Math.floor(twp.length / 5)) === 0 || i === twp.length - 1)
  const yTicks = [0, 25, 50, 75, 100]

  const topoNodes = [
    { icon: Zap, label: 'Apache Spark Engine', color: '#22d3ee', count: pm.spark, unit: 'msgs', bg: 'rgba(6,182,212,0.08)', details: ['8 Executors', '1,243 Tasks', '2.1 GB Shuffle'] },
    { icon: Radio, label: 'Apache Kafka Queue', color: '#a78bfa', count: pm.kafka, unit: 'msgs', bg: 'rgba(139,92,246,0.08)', details: ['6 Partitions', '3 Brokers', 'Offset lag: 142'] },
    { icon: Hash, label: 'HuggingFace API Broker', color: '#34d399', count: pm.hf, unit: 'reqs', bg: 'rgba(16,185,129,0.08)', details: ['Model: distilbert', 'Batch size: 32', 'Avg latency: 210ms'] },
  ]

  return (
    <div className="dashboard-container">
      {/* Animated background orbs */}
      <div className="orb-bg"><span className="o1" /><span className="o2" /><span className="o3" /></div>

      {/* ════ HEADER ════ */}
      <header className="header-glow relative z-10 bg-[#0a0e1a]/90 backdrop-blur-xl border-b border-white/5 h-14 flex items-center justify-between px-5">
        <div className="flex items-center gap-4">
          <button className="text-slate-400 hover:text-cyan-400 transition-colors"><Menu size={19} /></button>
          <div className="flex items-center gap-2.5">
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-cyan-500 to-purple-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
              <Globe size={14} className="text-white" />
            </div>
            <h1 className="text-white text-sm font-bold tracking-tight hidden sm:block">
              Real-Time Opinion Mining at Scale
            </h1>
          </div>
          <div className="hidden md:flex items-center gap-2 pl-4 ml-4 border-l border-white/10">
            <div className="w-6 h-6 rounded-full bg-gradient-to-br from-purple-500 to-pink-500 flex items-center justify-center">
              <span className="text-white text-[8px] font-bold">BD</span>
            </div>
            <span className="text-[11px] text-slate-400 font-medium">BDA Project</span>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 bg-white/5 px-3 py-1.5 rounded-full border border-white/5">
            <span className="pulse-dot" />
            <span className="text-[10px] font-semibold text-emerald-400 tracking-wide">LIVE</span>
            <span className="text-[9px] font-mono text-cyan-400 bg-cyan-500/10 px-1.5 py-0.5 rounded">{rate.toFixed(1)}M/s</span>
          </div>
          <button className="text-slate-500 hover:text-cyan-400 transition-colors"><HelpCircle size={16} /></button>
          <button className="text-slate-500 hover:text-cyan-400 transition-colors relative">
            <Bell size={16} />
            <span className="absolute -top-1.5 -right-1.5 w-3.5 h-3.5 bg-purple-500 rounded-full text-white text-[7px] font-bold flex items-center justify-center shadow-lg shadow-purple-500/40">{Math.min(99, Math.floor(totalTweets / 800))}</span>
          </button>
        </div>
      </header>

      {/* ════ MAIN GRID ════ */}
      <main className="relative z-10 max-w-[1440px] mx-auto p-5 lg:p-6 grid grid-cols-12 gap-4 lg:gap-5">

        {/* ───── KPI ROW ───── */}
        <div className="col-span-12 grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { icon: Database, label: 'Total Processed', val: fmt(totalTweets), unit: 'tweets', border: 'border-cyan', glow: 'glow-cyan', icol: 'text-cyan-400', bg: 'rgba(6,182,212,0.08)' },
            { icon: Activity, label: 'Streaming Rate', val: `${rate.toFixed(1)}M`, unit: '/sec', border: 'border-purple', glow: 'glow-purple', icol: 'text-purple-400', bg: 'rgba(139,92,246,0.08)' },
            { icon: Shield, label: 'Avg Confidence', val: `${avgConf.toFixed(1)}%`, unit: 'accuracy', border: 'border-green', glow: 'glow-green', icol: 'text-emerald-400', bg: 'rgba(16,185,129,0.08)' },
            { icon: AlertTriangle, label: 'Sentiment Mix', val: `${sp.pos.toFixed(1)}%`, unit: 'positive', border: 'border-cyan border-rotate', glow: 'glow-cyan', icol: 'text-cyan-400', bg: 'rgba(6,182,212,0.08)' },
          ].map((k, i) => (
            <div key={i} className={`glass p-4 lg:p-5 hover-lift ${k.border} ${k.glow}`} style={{ borderLeftWidth: '3px' }}>
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 rounded-xl flex items-center justify-center shrink-0" style={{ background: k.bg }}>
                  <k.icon size={18} className={k.icol} />
                </div>
                <div className="min-w-0">
                  <p className="text-[9px] font-semibold text-slate-500 uppercase tracking-[0.12em]">{k.label}</p>
                  <p className="text-lg font-bold text-white mt-0.5 font-mono tracking-tight kpi-shimmer">{k.val} <span className="text-[11px] font-medium text-slate-400 font-sans">{k.unit}</span></p>
                  {i === 3 && (
                    <div className="flex gap-2 mt-1.5 text-[10px]">
                      <span className="text-emerald-400 font-medium">+{fmt(sc.pos)}</span>
                      <span className="text-red-400 font-medium">-{fmt(sc.neg)}</span>
                      <span className="text-slate-400 font-medium">~{fmt(sc.neu)}</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* ───── MIDDLE LEFT: Distribution ───── */}
        <section className="col-span-12 lg:col-span-5 glass p-5">
          <div className="flex items-center gap-2 mb-4">
            <PieChart size={14} className="text-cyan-400" />
            <h2 className="text-[10px] font-bold text-slate-400 uppercase tracking-[0.15em]">Distribution Visualizer</h2>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Donut size={140} sw={22} segs={[{ v: sp.pos, c: '#34d399' }, { v: sp.neg, c: '#f87171' }, { v: sp.neu, c: '#475569' }]} label="Sentiment Split" />
            <Donut size={140} sw={22} segs={[{ v: sr.twitter, c: '#22d3ee' }, { v: sr.reddit, c: '#a78bfa' }, { v: sr.news, c: '#34d399' }, { v: sr.other, c: '#64748b' }]} label="Source Channels" />
          </div>
          <div className="flex flex-wrap justify-center gap-x-4 gap-y-1 mt-3 text-[10px] text-slate-400">
            <span><span className="inline-block w-2 h-2 rounded-full bg-emerald-400 mr-1.5 align-middle" /> Pos</span>
            <span><span className="inline-block w-2 h-2 rounded-full bg-red-400 mr-1.5 align-middle" /> Neg</span>
            <span><span className="inline-block w-2 h-2 rounded-full bg-slate-500 mr-1.5 align-middle" /> Neu</span>
            <span className="text-slate-600">|</span>
            <span><span className="inline-block w-2 h-2 rounded-full bg-cyan-400 mr-1.5 align-middle" /> X</span>
            <span><span className="inline-block w-2 h-2 rounded-full bg-purple-400 mr-1.5 align-middle" /> Red</span>
            <span><span className="inline-block w-2 h-2 rounded-full bg-emerald-400 mr-1.5 align-middle" /> News</span>
          </div>
        </section>

        {/* ───── MIDDLE RIGHT: Trends ───── */}
        <section className="col-span-12 lg:col-span-7 glass p-5">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <TrendingUp size={14} className="text-purple-400" />
              <h2 className="text-[10px] font-bold text-slate-400 uppercase tracking-[0.15em]">Stream Temporal Trends</h2>
            </div>
            <div className="flex bg-white/5 rounded-lg p-0.5 border border-white/5 gap-0.5">
              {['Month', 'Week', 'Day'].map(t => (
                <button key={t} onClick={() => setTimelineTab(t)}
                  className={`px-2.5 py-1 text-[10px] font-semibold rounded-md transition-all ${
                    timelineTab === t
                      ? 'bg-cyan-500/20 text-cyan-400 shadow-sm'
                      : 'text-slate-400 hover:text-white'
                  }`}>{t}</button>
              ))}
            </div>
          </div>
          <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto" preserveAspectRatio="xMidYMid meet">
            {yTicks.map(v => {
              const y = PT + ch - (v / 100) * ch
              return <g key={v}>
                <line x1={PL} y1={y} x2={W - PR} y2={y} stroke="rgba(148,163,184,0.06)" strokeWidth="1" />
                <text x={PL - 4} y={y + 3} textAnchor="end" className="text-[7px]" fill="#64748b">{v}%</text>
              </g>
            })}
            {xTicks.map(t => <text key={t.label} x={t.x} y={H - 4} textAnchor="middle" className="text-[6px]" fill="#64748b">{t.label}</text>)}
            <path d={fillArea(twp, PL, PT, sx, ch / 100, PT + ch)} fill="rgba(52,211,153,0.08)" />
            <path d={fillArea(twn, PL, PT, sx, ch / 100, PT + ch)} fill="rgba(148,163,184,0.05)" />
            <path d={fillArea(twng, PL, PT, sx, ch / 100, PT + ch)} fill="rgba(248,113,113,0.07)" />
            <path d={smoothPath(twp, PL, PT, sx, ch / 100)} fill="none" stroke="#34d399" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" filter="url(#glow)" />
            <path d={smoothPath(twn, PL, PT, sx, ch / 100)} fill="none" stroke="#64748b" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d={smoothPath(twng, PL, PT, sx, ch / 100)} fill="none" stroke="#f87171" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" filter="url(#glow2)" />
            <defs>
              <filter id="glow"><feGaussianBlur stdDeviation="1.5" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
              <filter id="glow2"><feGaussianBlur stdDeviation="1.5" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
            </defs>
          </svg>
          <div className="flex items-center justify-end gap-4 mt-1.5 text-[10px] text-slate-400">
            <span><span className="inline-block w-3 h-[2px] bg-emerald-400 mr-1.5 align-middle rounded" /> Positive</span>
            <span><span className="inline-block w-3 h-[2px] bg-slate-500 mr-1.5 align-middle rounded" /> Neutral</span>
            <span><span className="inline-block w-3 h-[2px] bg-red-400 mr-1.5 align-middle rounded" /> Negative</span>
          </div>
        </section>

        {/* ───── BOTTOM LEFT: Topology ───── */}
        <section className="col-span-12 lg:col-span-5 glass-strong overflow-hidden">
          <div className="px-5 py-3 flex items-center gap-2 border-b border-white/5 bg-gradient-to-r from-cyan-500/5 to-purple-500/5">
            <Layers size={13} className="text-purple-400" />
            <h2 className="text-[10px] font-bold text-slate-300 uppercase tracking-[0.15em]">Stream Topology</h2>
          </div>
          <div className="p-2">
            {topoNodes.map((node, idx) => {
              const open = expanded === idx
              const Icon = node.icon
              return (
                <div key={idx}>
                  <button onClick={() => setExpanded(open ? null : idx)}
                    className="topology-node w-full flex items-center justify-between px-3 py-2.5 rounded-xl text-left">
                    <div className="flex items-center gap-3 min-w-0">
                      <div className="w-8 h-8 rounded-lg flex items-center justify-center shrink-0" style={{ background: node.bg }}>
                        <Icon size={15} style={{ color: node.color }} />
                      </div>
                      <span className="text-sm font-medium text-slate-200">{node.label}</span>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      <span className="text-[10px] font-mono font-semibold text-slate-400 bg-white/5 px-2.5 py-1 rounded-full">{fmt(node.count)} {node.unit}</span>
                      {open ? <ChevronDown size={13} className="text-slate-500" /> : <ChevronRight size={13} className="text-slate-500" />}
                    </div>
                  </button>
                  {open && (
                    <div className="ml-14 mb-2 pl-3 border-l border-white/5 space-y-1 pb-1">
                      {node.details.map(d => (
                        <p key={d} className="text-[11px] text-slate-500 flex items-center gap-2">
                          <span className="w-1 h-1 rounded-full bg-slate-600 inline-block" />{d}</p>
                      ))}
                    </div>
                  )}
                  {idx < topoNodes.length - 1 && <hr className="border-white/5 mx-3" />}
                </div>
              )
            })}
          </div>
        </section>

        {/* ───── BOTTOM RIGHT: Stream Log ───── */}
        <section className="col-span-12 lg:col-span-7 glass p-5">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <BarChart3 size={13} className="text-emerald-400" />
              <h2 className="text-[10px] font-bold text-slate-400 uppercase tracking-[0.15em]">Recent Opinion Stream</h2>
            </div>
            <span className="text-[9px] text-slate-600 bg-white/5 px-2 py-1 rounded-full font-mono border border-white/5">
              {MAX_LOG} entries &middot; live
            </span>
          </div>
          <div className="overflow-x-auto table-wrap">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="border-b border-white/5">
                  <th className="text-[9px] font-bold text-slate-500 uppercase tracking-[0.12em] pb-3 pr-3">Tweet ID</th>
                  <th className="text-[9px] font-bold text-slate-500 uppercase tracking-[0.12em] pb-3 pr-3">Time</th>
                  <th className="text-[9px] font-bold text-slate-500 uppercase tracking-[0.12em] pb-3 pr-3">Sentiment</th>
                  <th className="text-[9px] font-bold text-slate-500 uppercase tracking-[0.12em] pb-3 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody>
                {logs.length === 0 ? (
                  <tr><td colSpan={4} className="py-10 text-center text-sm text-slate-600">
                    <Clock size={20} className="inline-block mb-2 text-slate-600" /><br />Awaiting stream...
                  </td></tr>
                ) : (
                  logs.map((row) => {
                    const isNew = feedKeys.has(row.key)
                    const barColor = row.sentiment === 'POSITIVE' ? '#34d399' : row.sentiment === 'NEGATIVE' ? '#f87171' : '#64748b'
                    return (
                      <tr key={row.key} className={`border-b border-white/[0.03] last:border-0 transition-colors ${isNew ? 'feed-row-new' : ''}`}>
                        <td className="py-2.5 pr-3">
                          <code className="text-[11px] font-mono text-cyan-300 bg-cyan-500/5 px-2 py-0.5 rounded border border-cyan-500/10">{row.id}</code>
                        </td>
                        <td className="py-2.5 pr-3 text-[12px] text-slate-400 font-mono">{new Date(row.timestamp).toLocaleTimeString('en-US', { hour12: false })}</td>
                        <td className="py-2.5 pr-3"><SentBadge label={row.sentiment} /></td>
                        <td className="py-2.5 text-right">
                          <div className="flex items-center justify-end gap-2.5">
                            <div className="w-14 h-1 bg-white/5 rounded-full overflow-hidden">
                              <div className="h-full rounded-full transition-all duration-300" style={{ width: `${row.confidence}%`, background: barColor }} />
                            </div>
                            <span className="text-xs font-bold text-slate-300 font-mono w-12 text-right">{row.confidence.toFixed(1)}%</span>
                          </div>
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  )
}
