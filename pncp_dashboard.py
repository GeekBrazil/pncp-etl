"""
PNCP Control Dashboard — FastAPI + SSE + PDF
Porta 8790
"""
import asyncio, json, threading, time, io
import queue as _queue
from datetime import date, timedelta
from typing import Optional
import psycopg2, psycopg2.extras
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from sse_starlette.sse import EventSourceResponse
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

app = FastAPI(title="PNCP Control")

DB = dict(host="core-postgres", port=5432, dbname="pncp_db", user="allan", password="Alcachofra")
UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]
MODALIDADES = {6:"Pregão Eletrônico",7:"Pregão Presencial",8:"Dispensa",
               9:"Inexigibilidade",4:"Concorrência Eletrônica",5:"Concorrência Presencial",
               12:"Credenciamento",3:"Concurso",1:"Leilão Eletrônico"}

_etl_queue: Optional[_queue.Queue] = None
_etl_task: Optional[threading.Thread] = None

def db():
    return psycopg2.connect(**DB)

def query(sql, params=None):
    conn = db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ─── HTML ─────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PNCP Control</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07070f;--bg2:#0f0f1a;--bg3:#15152a;
  --border:rgba(99,102,241,.18);
  --accent:#6366f1;--accent2:#818cf8;
  --text:#e2e2f0;--muted:#8b8ba8;
  --green:#22c55e;--yellow:#eab308;--red:#ef4444
}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;display:flex;flex-direction:column}

/* HEADER */
header{
  height:44px;flex-shrink:0;
  background:rgba(7,7,15,.95);backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);
  padding:0 1.2rem;
  display:flex;align-items:center;justify-content:space-between;z-index:100
}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:1.05rem}
.logo span{color:var(--accent)}
.header-right{font-size:.7rem;color:var(--muted)}

/* MAIN LAYOUT */
.layout{flex:1;overflow:hidden;display:grid;grid-template-columns:256px 1fr}

/* LEFT PANEL */
.left-panel{
  background:var(--bg2);border-right:1px solid var(--border);
  overflow-y:auto;padding:.8rem;display:flex;flex-direction:column;gap:.75rem
}
.panel-title{
  font-family:'Syne',sans-serif;font-size:.75rem;font-weight:700;
  color:var(--accent2);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.35rem
}
.form-group{margin-bottom:.55rem}
label.fl{display:block;font-size:.66rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:.22rem}
input[type=date],select{
  width:100%;background:var(--bg3);border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:.42rem .65rem;font-family:inherit;font-size:.8rem;outline:none
}
input[type=date]:focus,select:focus{border-color:var(--accent)}
.ufs-wrap{
  max-height:96px;overflow-y:auto;border:1px solid var(--border);
  border-radius:7px;padding:.4rem .5rem;background:var(--bg3)
}
.ufs-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.18rem}
.uf-cb{display:flex;align-items:center;gap:.22rem;font-size:.72rem;cursor:pointer;white-space:nowrap}
.uf-cb input{accent-color:var(--accent);cursor:pointer;width:11px;height:11px}
.mods-wrap{
  max-height:86px;overflow-y:auto;border:1px solid var(--border);
  border-radius:7px;padding:.4rem .5rem;background:var(--bg3)
}
.mods-grid{display:grid;grid-template-columns:1fr 1fr;gap:.18rem}
.mod-cb{display:flex;align-items:center;gap:.22rem;font-size:.7rem;cursor:pointer}
.mod-cb input{accent-color:var(--accent);width:11px;height:11px}
.btn{display:block;width:100%;padding:.55rem;border:none;border-radius:8px;font-family:'Syne',sans-serif;font-size:.85rem;font-weight:700;cursor:pointer;transition:opacity .2s}
.btn-primary{background:var(--accent);color:#fff}.btn-primary:hover{opacity:.85}
.btn-primary:disabled{opacity:.4;cursor:default}
.btn-outline{background:transparent;color:var(--accent2);border:1px solid var(--border)}.btn-outline:hover{border-color:var(--accent)}
.row-btns{display:flex;gap:.45rem}
.row-btns .btn{flex:1}

/* PROGRESS */
#progress-wrap{display:none;margin-top:.4rem}
.prog-bar-wrap{background:var(--bg3);border-radius:4px;height:4px;overflow:hidden;margin:.35rem 0}
.prog-bar{height:100%;background:var(--accent);border-radius:4px;transition:width .3s;width:0%}
#log-box{
  background:var(--bg3);border:1px solid var(--border);border-radius:8px;
  padding:.55rem;font-family:'Courier New',monospace;font-size:.68rem;
  height:140px;overflow-y:auto;line-height:1.5
}
.log-ok{color:var(--green)}.log-err{color:var(--red)}.log-info{color:var(--accent2)}.log-muted{color:var(--muted)}

/* HIST */
.hist-card{
  background:var(--bg3);border:1px solid var(--border);border-radius:8px;
  padding:.55rem;font-size:.72rem;max-height:110px;overflow-y:auto
}
.hist-row{display:flex;justify-content:space-between;align-items:center;padding:.28rem 0;border-bottom:1px solid rgba(99,102,241,.1)}
.hist-row:last-child{border:none}
.badge{padding:.12rem .42rem;border-radius:12px;font-size:.62rem;font-weight:600}
.badge-ok{background:rgba(34,197,94,.15);color:var(--green)}
.badge-err{background:rgba(239,68,68,.15);color:var(--red)}

/* RIGHT PANEL */
.right-panel{display:flex;flex-direction:column;overflow:hidden;padding:.8rem;gap:.65rem}

/* KPIs */
.kpis-bar{display:flex;gap:.6rem;flex-shrink:0}
.kpi{
  background:var(--bg2);border:1px solid var(--border);border-radius:10px;
  padding:.5rem .8rem;flex:1;display:flex;flex-direction:column;align-items:center
}
.kpi .label{font-size:.62rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.12rem}
.kpi .val{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--accent2);line-height:1}
.kpi .val.green{color:var(--green)}.kpi .val.yellow{color:var(--yellow)}

/* TABLE CARD */
.table-card{
  flex:1;overflow:hidden;
  background:var(--bg2);border:1px solid var(--border);border-radius:12px;
  display:flex;flex-direction:column;padding:.8rem
}
.table-header{display:flex;align-items:center;gap:.55rem;flex-shrink:0;margin-bottom:.6rem;flex-wrap:wrap}
.table-header h2{font-family:'Syne',sans-serif;font-size:.78rem;font-weight:700;color:var(--accent2);text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}
.search-bar{display:flex;gap:.45rem;flex:1;align-items:center;min-width:0}
.search-bar input{
  flex:1;min-width:0;background:var(--bg3);border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:.42rem .65rem;font-family:inherit;font-size:.8rem;outline:none
}
.search-bar input:focus{border-color:var(--accent)}
.search-bar select{
  width:120px;flex-shrink:0;background:var(--bg3);border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:.42rem .65rem;font-family:inherit;font-size:.8rem;outline:none
}
.action-btns{display:flex;gap:.45rem;flex-shrink:0}
.btn-sm{padding:.38rem .75rem;border:none;border-radius:7px;font-family:'Syne',sans-serif;font-size:.76rem;font-weight:700;cursor:pointer;white-space:nowrap}
.btn-sm-outline{background:transparent;color:var(--accent2);border:1px solid var(--border)}.btn-sm-outline:hover{border-color:var(--accent)}
.btn-sm-green{background:#16a34a;color:#fff}.btn-sm-green:hover{opacity:.87}
.count-badge{font-size:.7rem;color:var(--muted);white-space:nowrap;flex-shrink:0}

/* FILTER ROW 2 */
.filter-row2{
  display:flex;gap:.45rem;align-items:center;flex-shrink:0;flex-wrap:wrap;
  padding-top:.45rem;border-top:1px solid var(--border);
}
.status-chips{display:flex;gap:.3rem}
.chip{
  padding:.3rem .65rem;border-radius:20px;font-size:.72rem;font-weight:500;
  background:var(--bg3);border:1px solid var(--border);color:var(--muted);cursor:pointer;transition:all .18s
}
.chip.active{background:rgba(99,102,241,.18);border-color:rgba(99,102,241,.5);color:var(--accent2)}
.chip:hover:not(.active){border-color:rgba(99,102,241,.3);color:var(--text)}
.chip.green.active{background:rgba(34,197,94,.15);border-color:rgba(34,197,94,.4);color:var(--green)}
.chip.red.active{background:rgba(239,68,68,.12);border-color:rgba(239,68,68,.35);color:var(--red)}
.val-range{display:flex;align-items:center;gap:.3rem}
.val-range input{
  width:100px;background:var(--bg3);border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:.3rem .6rem;font-family:inherit;font-size:.76rem;outline:none
}
.val-range input:focus{border-color:var(--accent)}
.val-range span{font-size:.72rem;color:var(--muted)}
.filter-row2 select{
  background:var(--bg3);border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:.3rem .6rem;font-family:inherit;font-size:.76rem;outline:none;cursor:pointer
}
.filter-row2 select:focus{border-color:var(--accent)}
.row2-spacer{flex:1}

/* TABLE */
.table-wrap{flex:1;overflow-y:auto;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:.79rem}
th{
  background:var(--bg3);padding:.5rem .8rem;text-align:left;
  font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.04em;
  white-space:nowrap;position:sticky;top:0;cursor:pointer;user-select:none;z-index:1
}
th:hover{color:var(--accent2)}
td{padding:.48rem .8rem;border-top:1px solid var(--border);vertical-align:middle}
tr:hover td{background:rgba(99,102,241,.05)}
.td-val{color:var(--green);font-weight:600;white-space:nowrap;text-align:right}
.td-link a{color:var(--accent2);text-decoration:none;font-size:.72rem}.td-link a:hover{text-decoration:underline}
.td-obj{max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.td-date{color:var(--yellow);white-space:nowrap;font-size:.73rem}
.td-muted{color:var(--text);font-size:.76rem}

@media(max-width:800px){
  .layout{grid-template-columns:1fr;grid-template-rows:auto 1fr}
  html,body{height:auto;overflow:auto}
  .left-panel{max-height:280px}
}
</style>
</head>
<body>
<header>
  <div class="logo">PNCP <span>Control</span></div>
  <div class="header-right">Plataforma Municipal · <span id="last-update">—</span></div>
</header>
<div class="layout">

  <!-- LEFT: Controle ETL -->
  <div class="left-panel">
    <div>
      <div class="panel-title">⚙ Controle ETL</div>
      <div class="form-group">
        <label class="fl">Data Inicial</label>
        <input type="date" id="data-ini">
      </div>
      <div class="form-group">
        <label class="fl">Data Final</label>
        <input type="date" id="data-fim">
      </div>
      <div class="form-group">
        <label class="fl">
          Estados
          <span style="float:right;cursor:pointer;color:var(--accent);font-size:.65rem" onclick="toggleAll()">Todos/Nenhum</span>
        </label>
        <div class="ufs-wrap"><div class="ufs-grid" id="ufs-grid"></div></div>
      </div>
      <div class="form-group">
        <label class="fl">Modalidades</label>
        <div class="mods-wrap"><div class="mods-grid" id="mods-grid"></div></div>
      </div>
      <div class="row-btns">
        <button class="btn btn-primary" id="btn-run" onclick="runETL()">▶ Executar</button>
        <button class="btn btn-outline" onclick="stopETL()">■ Parar</button>
      </div>
      <div id="progress-wrap">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.15rem">
          <span style="font-size:.7rem;color:var(--muted)" id="prog-label">—</span>
        </div>
        <div class="prog-bar-wrap"><div class="prog-bar" id="prog-bar"></div></div>
        <div id="log-box"></div>
      </div>
    </div>

    <div>
      <div class="panel-title">📋 Histórico</div>
      <div class="hist-card" id="hist-list"><span style="color:var(--muted)">Carregando...</span></div>
    </div>
  </div>

  <!-- RIGHT: Licitações -->
  <div class="right-panel">

    <!-- KPIs compactos -->
    <div class="kpis-bar">
      <div class="kpi"><div class="label">Licitações</div><div class="val" id="k-total">—</div></div>
      <div class="kpi"><div class="label">Valor Total</div><div class="val yellow" id="k-valor">—</div></div>
      <div class="kpi"><div class="label">Estados</div><div class="val" id="k-estados">—</div></div>
      <div class="kpi"><div class="label">Abertas</div><div class="val green" id="k-abertas">—</div></div>
    </div>

    <!-- Tabela principal -->
    <div class="table-card">
      <div class="table-header">
        <h2>📊 Licitações</h2>
        <div class="search-bar">
          <input type="text" id="search" placeholder="Buscar objeto, município, órgão..." oninput="filtrar()">
          <select id="filter-uf" onchange="filtrar()"><option value="">Todos estados</option></select>
          <select id="filter-mod" onchange="filtrar()"><option value="">Todas modalidades</option></select>
          <span class="count-badge" id="count-badge"></span>
        </div>
        <div class="action-btns">
          <button class="btn-sm btn-sm-outline" onclick="loadDados()" title="Atualizar">🔄</button>
          <button class="btn-sm btn-sm-green" onclick="gerarPDF()">📄 PDF</button>
        </div>
      </div>
      <!-- Filtros avançados linha 2 -->
      <div class="filter-row2">
        <div class="status-chips">
          <button class="chip active" data-status="" onclick="setStatus(this)">Todas</button>
          <button class="chip green" data-status="aberta" onclick="setStatus(this)">🟢 Abertas</button>
          <button class="chip red" data-status="encerrada" onclick="setStatus(this)">⚫ Enc.</button>
        </div>
        <div class="val-range">
          <span>R$</span>
          <input type="number" id="val-min" placeholder="Min" min="0" step="10000" oninput="filtrar()" title="Valor mínimo">
          <span>—</span>
          <input type="number" id="val-max" placeholder="Máx" min="0" step="10000" oninput="filtrar()" title="Valor máximo">
        </div>
        <select id="filter-order" onchange="filtrar()" title="Ordenar por">
          <option value="valor_desc">↓ Maior valor</option>
          <option value="valor_asc">↑ Menor valor</option>
          <option value="data_desc">↓ Mais recentes</option>
          <option value="enc_asc">⏰ Encerra em breve</option>
          <option value="municipio">📍 Município A-Z</option>
        </select>
        <div class="val-range">
          <span style="font-size:.7rem;color:var(--muted)">Pub:</span>
          <input type="date" id="filter-data-ini" oninput="filtrar()" style="width:120px">
          <span>—</span>
          <input type="date" id="filter-data-fim" oninput="filtrar()" style="width:120px">
        </div>
        <div class="row2-spacer"></div>
        <button class="chip" onclick="limparFiltros()" style="flex-shrink:0">✕ Limpar</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th onclick="sortBy('municipio_nome')">Município ↕</th>
            <th>Órgão</th>
            <th>Objeto</th>
            <th onclick="sortBy('valor_estimado')" style="text-align:right">Valor ↕</th>
            <th onclick="sortBy('modalidade_nome')">Modalidade ↕</th>
            <th onclick="sortBy('data_encerramento')">Encerra ↕</th>
            <th>Edital</th>
          </tr></thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"];
const MODS = {6:"Pregão Eletr.",7:"Pregão Pres.",8:"Dispensa",9:"Inexigibilidade",4:"Concorrência Eletr.",5:"Concorrência Pres.",12:"Credenciamento",3:"Concurso",1:"Leilão"};

let allDados = [], sortField = 'valor_estimado', sortAsc = false, evtSource = null;
let activeStatus = '';

window.onload = () => {
  const hoje = new Date(), ini = new Date(hoje);
  ini.setDate(ini.getDate() - 7);
  document.getElementById('data-ini').value = ini.toISOString().slice(0,10);
  document.getElementById('data-fim').value = hoje.toISOString().slice(0,10);

  const ufGrid = document.getElementById('ufs-grid');
  UFS.forEach(uf => {
    const l = document.createElement('label');
    l.className = 'uf-cb';
    l.innerHTML = `<input type="checkbox" class="uf-cb-inp" value="${uf}" checked> ${uf}`;
    ufGrid.appendChild(l);
  });

  const modGrid = document.getElementById('mods-grid');
  Object.entries(MODS).forEach(([id,nome]) => {
    const l = document.createElement('label');
    l.className = 'mod-cb';
    l.innerHTML = `<input type="checkbox" class="mod-cb-inp" value="${id}" checked> ${nome}`;
    modGrid.appendChild(l);
  });

  loadKPIs(); loadDados(); loadHist();
};

function toggleAll() {
  const cbs = document.querySelectorAll('.uf-cb-inp');
  const anyChecked = [...cbs].some(c => c.checked);
  cbs.forEach(c => c.checked = !anyChecked);
}

async function loadKPIs() {
  const r = await fetch('/kpis');
  const d = await r.json();
  document.getElementById('k-total').textContent = (d.total || 0).toLocaleString('pt-BR');
  const v = parseFloat(d.valor || 0);
  let vStr = '—';
  if (v >= 1e9) vStr = 'R$ ' + (v/1e9).toFixed(1).replace('.',',') + ' Bi';
  else if (v >= 1e6) vStr = 'R$ ' + Math.round(v/1e6).toLocaleString('pt-BR') + ' M';
  else if (v > 0) vStr = 'R$ ' + v.toLocaleString('pt-BR', {maximumFractionDigits:0});
  document.getElementById('k-valor').textContent = vStr;
  document.getElementById('k-estados').textContent = d.estados || '—';
  document.getElementById('k-abertas').textContent = (d.abertas || 0).toLocaleString('pt-BR');
  document.getElementById('last-update').textContent = new Date().toLocaleTimeString('pt-BR');
}

async function loadDados() {
  const r = await fetch('/dados?limit=5000');
  allDados = await r.json();
  const ufSel = document.getElementById('filter-uf');
  const modSel = document.getElementById('filter-mod');
  const ufs = [...new Set(allDados.map(d => d.uf).filter(Boolean))].sort();
  const mods = [...new Set(allDados.map(d => d.modalidade_nome).filter(Boolean))].sort();
  ufSel.innerHTML = '<option value="">Todos estados</option>' + ufs.map(u => `<option>${u}</option>`).join('');
  modSel.innerHTML = '<option value="">Todas modalidades</option>' + mods.map(m => `<option>${m}</option>`).join('');
  filtrar();
}

function setStatus(btn) {
  document.querySelectorAll('.status-chips .chip').forEach(c => c.classList.remove('active'));
  btn.classList.add('active');
  activeStatus = btn.dataset.status;
  filtrar();
}

function filtrar() {
  const q     = document.getElementById('search').value.toLowerCase();
  const uf    = document.getElementById('filter-uf').value;
  const mod   = document.getElementById('filter-mod').value;
  const vMin  = parseFloat(document.getElementById('val-min').value) || 0;
  const vMax  = parseFloat(document.getElementById('val-max').value) || 0;
  const order = document.getElementById('filter-order').value;
  const dIni  = document.getElementById('filter-data-ini').value;
  const dFim  = document.getElementById('filter-data-fim').value;
  const hoje  = new Date().toISOString().slice(0, 10);

  let rows = allDados.filter(d => {
    if (q && ![d.municipio_nome, d.orgao_nome, d.objeto].join(' ').toLowerCase().includes(q)) return false;
    if (uf  && d.uf !== uf) return false;
    if (mod && d.modalidade_nome !== mod) return false;
    if (vMin > 0 && (d.valor_estimado || 0) < vMin) return false;
    if (vMax > 0 && (d.valor_estimado || 0) > vMax) return false;
    if (dIni && d.data_publicacao && d.data_publicacao < dIni) return false;
    if (dFim && d.data_publicacao && d.data_publicacao > dFim) return false;
    if (activeStatus === 'aberta'    && (!d.data_encerramento || d.data_encerramento < hoje)) return false;
    if (activeStatus === 'encerrada' && d.data_encerramento && d.data_encerramento >= hoje) return false;
    return true;
  });

  rows.sort((a, b) => {
    switch (order) {
      case 'valor_asc':  return (a.valor_estimado||0) - (b.valor_estimado||0);
      case 'data_desc':  return (b.data_publicacao||'').localeCompare(a.data_publicacao||'');
      case 'enc_asc': {
        const ae = a.data_encerramento || '9999-99-99';
        const be = b.data_encerramento || '9999-99-99';
        return ae.localeCompare(be);
      }
      case 'municipio': return (a.municipio_nome||'').localeCompare(b.municipio_nome||'');
      default:           return (b.valor_estimado||0) - (a.valor_estimado||0);
    }
  });

  renderTable(rows);

  // KPIs dinâmicos refletem a seleção atual
  const vTotal  = rows.reduce((s, d) => s + (d.valor_estimado || 0), 0);
  const abertas = rows.filter(d => d.data_encerramento && d.data_encerramento >= hoje).length;
  document.getElementById('k-total').textContent   = rows.length.toLocaleString('pt-BR');
  document.getElementById('k-abertas').textContent = abertas.toLocaleString('pt-BR');
  if (vTotal >= 1e9)
    document.getElementById('k-valor').textContent = 'R$ ' + (vTotal/1e9).toFixed(1).replace('.',',') + ' Bi';
  else
    document.getElementById('k-valor').textContent = 'R$ ' + Math.round(vTotal/1e6).toLocaleString('pt-BR') + ' M';
  document.getElementById('count-badge').textContent = rows.length.toLocaleString('pt-BR') + ' resultados';
}

function sortBy(f) {
  const sel = document.getElementById('filter-order');
  if (f === 'valor_estimado')   sel.value = sel.value === 'valor_desc' ? 'valor_asc' : 'valor_desc';
  else if (f === 'data_encerramento') sel.value = 'enc_asc';
  else if (f === 'municipio_nome')    sel.value = 'municipio';
  filtrar();
}

function limparFiltros() {
  document.getElementById('search').value = '';
  document.getElementById('filter-uf').value = '';
  document.getElementById('filter-mod').value = '';
  document.getElementById('val-min').value = '';
  document.getElementById('val-max').value = '';
  document.getElementById('filter-order').value = 'valor_desc';
  document.getElementById('filter-data-ini').value = '';
  document.getElementById('filter-data-fim').value = '';
  document.querySelectorAll('.status-chips .chip').forEach((c, i) => c.classList.toggle('active', i === 0));
  activeStatus = '';
  filtrar();
}

function renderTable(rows) {
  const brl = v => v ? 'R$ ' + Number(v).toLocaleString('pt-BR', {minimumFractionDigits:0, maximumFractionDigits:0}) : '—';
  const esc = s => String(s || '—').replace(/</g, '&lt;');
  const modAbrev = s => (s||'').replace('Eletrônico','Eletr.').replace('Presencial','Pres.').replace('Concorrência','Concorr.');
  document.getElementById('tbody').innerHTML = rows.map(d => `
    <tr>
      <td class="td-muted">${esc(d.municipio_nome || d.uf || '—')}</td>
      <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.73rem;color:var(--muted)" title="${esc(d.orgao_nome)}">${esc((d.orgao_nome||'').slice(0,30))}</td>
      <td class="td-obj" title="${esc(d.objeto)}">${esc((d.objeto||'').slice(0,65))}</td>
      <td class="td-val">${brl(d.valor_estimado)}</td>
      <td class="td-muted" style="font-size:.72rem">${esc(modAbrev(d.modalidade_nome))}</td>
      <td class="td-date">${d.data_encerramento || '—'}</td>
      <td class="td-link">${d.url_pncp ? `<a href="${d.url_pncp}" target="_blank" rel="noopener">↗ edital</a>` : '—'}</td>
    </tr>`).join('');
}

async function loadHist() {
  const r = await fetch('/etl/status');
  const rows = await r.json();
  document.getElementById('hist-list').innerHTML = rows.length
    ? rows.slice(0, 10).map(r => `
      <div class="hist-row">
        <div>
          <span style="font-weight:600">${r.uf || 'ALL'}</span>
          <span style="color:var(--muted);font-size:.65rem;margin-left:.35rem">${(r.executado_em||'').slice(0,16).replace('T',' ')}</span>
        </div>
        <div style="display:flex;align-items:center;gap:.4rem">
          <span style="color:var(--green);font-size:.68rem">${r.inseridos || 0}</span>
          <span class="badge ${r.status === 'ok' ? 'badge-ok' : 'badge-err'}">${r.status}</span>
        </div>
      </div>`).join('')
    : '<span style="color:var(--muted)">Nenhuma execução</span>';
}

async function runETL() {
  const ufs = [...document.querySelectorAll('.uf-cb-inp:checked')].map(c => c.value);
  const mods = [...document.querySelectorAll('.mod-cb-inp:checked')].map(c => parseInt(c.value));
  if (!ufs.length) { alert('Selecione ao menos 1 estado'); return; }

  document.getElementById('btn-run').disabled = true;
  document.getElementById('progress-wrap').style.display = 'block';
  document.getElementById('log-box').innerHTML = '';
  document.getElementById('prog-bar').style.width = '0%';
  document.getElementById('prog-label').textContent = 'Iniciando...';

  const body = {
    ufs, modalidades: mods,
    data_ini: document.getElementById('data-ini').value,
    data_fim: document.getElementById('data-fim').value
  };

  await fetch('/etl/run', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });

  if (evtSource) evtSource.close();
  evtSource = new EventSource('/etl/stream');
  let total = ufs.length * mods.length, done = 0;

  evtSource.onmessage = e => {
    const log = document.getElementById('log-box');
    const data = e.data;
    let cls = 'log-info';
    if (data.includes('✅') || data.includes('inseridos')) cls = 'log-ok';
    else if (data.includes('❌') || data.includes('ERRO')) cls = 'log-err';
    else if (data.includes('sem dados') || data.includes('vazio') || data.includes('—')) cls = 'log-muted';
    if (data.includes('→')) done++;
    const pct = Math.min(99, Math.round(done / total * 100));
    document.getElementById('prog-bar').style.width = pct + '%';
    document.getElementById('prog-label').textContent = data.replace(/\[.*?\]/, '').trim().slice(0, 52);
    log.innerHTML += `<div class="${cls}">${data}</div>`;
    log.scrollTop = log.scrollHeight;
    if (data.startsWith('DONE')) {
      evtSource.close(); evtSource = null;
      document.getElementById('btn-run').disabled = false;
      document.getElementById('prog-bar').style.width = '100%';
      document.getElementById('prog-label').textContent = '✅ Concluído';
      setTimeout(() => { loadKPIs(); loadDados(); loadHist(); }, 500);
    }
  };
  evtSource.onerror = () => {
    if (evtSource) { evtSource.close(); evtSource = null; }
    document.getElementById('btn-run').disabled = false;
    loadKPIs(); loadDados(); loadHist();
  };
}

function stopETL() {
  fetch('/etl/stop', {method: 'POST'});
  if (evtSource) { evtSource.close(); evtSource = null; }
  document.getElementById('btn-run').disabled = false;
  document.getElementById('prog-label').textContent = '⏹ Parado pelo usuário';
}

function gerarPDF() {
  const uf = document.getElementById('filter-uf').value;
  window.open('/pdf' + (uf ? '?uf=' + uf : ''), '_blank');
}
</script>
</body>
</html>"""

# ─── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(): return HTML

@app.get("/kpis")
async def kpis():
    rows = query("""
        SELECT COUNT(*) AS total,
               SUM(valor_estimado) AS valor,
               COUNT(DISTINCT uf) AS estados,
               COUNT(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas
        FROM licitacoes
    """)
    return rows[0] if rows else {}

@app.get("/dados")
async def dados(limit: int = 5000, uf: str = None, modalidade: int = None):
    sql = "SELECT * FROM licitacoes WHERE 1=1"
    params = []
    if uf: sql += " AND uf=%s"; params.append(uf)
    if modalidade: sql += " AND modalidade_id=%s"; params.append(modalidade)
    sql += " ORDER BY valor_estimado DESC LIMIT %s"; params.append(limit)
    return query(sql, params)

@app.get("/etl/status")
async def etl_status():
    return query("SELECT * FROM etl_log ORDER BY id DESC LIMIT 20")

# ─── ETL assíncrono com SSE (thread-safe via queue.Queue) ─────────────────────
@app.post("/etl/run")
async def etl_run(request: Request):
    global _etl_task, _etl_queue
    body = await request.json()
    _etl_queue = _queue.Queue()
    _etl_task = threading.Thread(target=_run_etl_thread, args=(_etl_queue, body), daemon=True)
    _etl_task.start()
    return {"ok": True}

def _run_etl_thread(q: _queue.Queue, params: dict):
    import sys
    sys.path.insert(0, "/app")
    from etl import MODALIDADES as ALL_MODS, fetch_page, normalizar, inserir_licitacoes
    from etl import salvar_log, calcular_top_palavras, inserir_top_palavras, get_conn
    import time as _time

    ufs = params.get("ufs", ["RJ"])
    mods_req = set(params.get("modalidades", list(ALL_MODS.keys())))
    data_ini = params.get("data_ini", str(date.today() - timedelta(days=2)))
    data_fim = params.get("data_fim", str(date.today()))

    q.put(f"[INFO] Iniciando: {len(ufs)} estados · {data_ini} → {data_fim}")

    for uf in ufs:
        q.put(f"\n📥 {uf}")
        inicio = _time.time()
        conn = get_conn()
        total_ins = total_dup = erros = paginas = 0
        todos = []
        mods_to_run = {k: v for k, v in ALL_MODS.items() if k in mods_req}

        for mod_id, mod_nome in mods_to_run.items():
            q.put(f"  [{mod_id:02d}] {mod_nome[:28]}")
            mod_ins = 0
            for pag in range(1, 6):
                data = fetch_page(uf, data_ini, data_fim, mod_id, pag)
                if not data:
                    break
                items = data.get("data") or []
                total_pag = int(data.get("totalPaginas") or 1)
                if not items:
                    break
                regs = []
                for l in items:
                    try: regs.append(normalizar(l, uf))
                    except: erros += 1
                ins, dup = inserir_licitacoes(conn, regs)
                total_ins += ins; total_dup += dup; mod_ins += ins
                todos += regs; paginas += 1
                if pag >= total_pag:
                    break
                _time.sleep(0.4)
            if mod_ins > 0:
                q.put(f"  → {mod_ins} inseridos")
            _time.sleep(0.2)

        top = calcular_top_palavras(todos, uf, data_fim[:7])
        inserir_top_palavras(conn, top)
        dur = _time.time() - inicio
        status = "ok" if erros == 0 else "parcial"
        salvar_log(conn, uf, data_ini, data_fim, paginas, total_ins, total_dup, erros, dur, status)
        conn.close()
        q.put(f"✅ {uf}: {total_ins} inseridos ({dur:.0f}s)")

    q.put("DONE Concluído")

@app.get("/etl/stream")
async def etl_stream():
    async def generator():
        global _etl_queue
        if not _etl_queue:
            yield {"data": "Nenhum ETL em execução"}
            return
        while True:
            try:
                msg = _etl_queue.get_nowait()
                yield {"data": msg}
                if msg.startswith("DONE"):
                    break
            except _queue.Empty:
                await asyncio.sleep(0.2)
    return EventSourceResponse(generator())

@app.post("/etl/stop")
async def etl_stop():
    global _etl_queue
    if _etl_queue:
        _etl_queue.put("DONE Parado")
    return {"ok": True}

# ─── PDF ──────────────────────────────────────────────────────────────────────
@app.get("/pdf")
async def gerar_pdf(uf: str = None, limite: int = 100):
    rows = query(
        f"SELECT municipio_nome, uf, orgao_nome, objeto, valor_estimado, modalidade_nome, data_encerramento, url_pncp "
        f"FROM licitacoes {'WHERE uf=%s' if uf else ''} ORDER BY valor_estimado DESC LIMIT %s",
        ([uf, limite] if uf else [limite])
    )
    kpi = query("""SELECT COUNT(*) total, SUM(valor_estimado) valor,
                   COUNT(DISTINCT uf) estados,
                   COUNT(*) FILTER (WHERE data_encerramento>=CURRENT_DATE) abertas
                   FROM licitacoes""")[0]

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    title_s = ParagraphStyle("t", parent=styles["Title"], fontSize=18,
                              textColor=colors.HexColor("#6366f1"), spaceAfter=6)
    sub_s   = ParagraphStyle("s", parent=styles["Normal"], fontSize=9,
                              textColor=colors.grey, spaceAfter=12)
    h2_s    = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11,
                              textColor=colors.HexColor("#6366f1"), spaceBefore=14, spaceAfter=6)
    cell_s  = ParagraphStyle("c", parent=styles["Normal"], fontSize=7, leading=9)

    def brl(v):
        return f"R$ {float(v or 0):,.0f}".replace(",","X").replace(".",",").replace("X",".")

    story = [
        Paragraph("PNCP — Relatório de Licitações", title_s),
        Paragraph(f"{'UF: '+uf if uf else 'Todos os estados'} · {date.today().strftime('%d/%m/%Y')} · {len(rows)} licitações", sub_s),
        HRFlowable(width="100%", color=colors.HexColor("#6366f1"), spaceAfter=12),
    ]

    v = float(kpi['valor'] or 0)
    if v >= 1e9: v_fmt = f"R$ {v/1e9:.1f} Bi"
    elif v >= 1e6: v_fmt = f"R$ {v/1e6:.0f} M"
    else: v_fmt = brl(v)

    kpi_data = [["Total","Valor Total","Estados","Abertas"],
                [f"{kpi['total']:,}", v_fmt, str(kpi['estados']), str(kpi['abertas'])]]
    kpi_t = Table(kpi_data, colWidths=[4*cm]*4)
    kpi_t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#15152a")),
        ("TEXTCOLOR",(0,0),(-1,0),colors.HexColor("#818cf8")),
        ("FONTSIZE",(0,0),(-1,0),8), ("FONTSIZE",(0,1),(-1,1),13),
        ("FONTNAME",(0,1),(-1,1),"Helvetica-Bold"),
        ("TEXTCOLOR",(0,1),(-1,1),colors.HexColor("#22c55e")),
        ("ALIGN",(0,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("ROWBACKGROUNDS",(0,1),(-1,1),[colors.HexColor("#0f0f1a")]),
        ("BOX",(0,0),(-1,-1),.5,colors.HexColor("#6366f1")),
        ("INNERGRID",(0,0),(-1,-1),.3,colors.HexColor("#15152a")),
        ("TOPPADDING",(0,0),(-1,-1),8), ("BOTTOMPADDING",(0,0),(-1,-1),8),
    ]))
    story += [kpi_t, Spacer(1, 14), Paragraph("Licitações", h2_s)]

    headers = ["Município","Órgão","Objeto","Valor","Modalidade","Encerra","Edital"]
    data = [headers]
    for r in rows:
        data.append([
            Paragraph(str(r["municipio_nome"] or r["uf"] or "—")[:30], cell_s),
            Paragraph(str(r["orgao_nome"] or "")[:40], cell_s),
            Paragraph(str(r["objeto"] or "")[:80], cell_s),
            Paragraph(brl(r["valor_estimado"]), cell_s),
            Paragraph(str(r["modalidade_nome"] or "")[:20], cell_s),
            Paragraph(str(r["data_encerramento"] or "—"), cell_s),
            Paragraph(str(r["url_pncp"] or "—")[:40], cell_s),
        ])

    t = Table(data, colWidths=[2.5*cm,3.5*cm,5*cm,2.2*cm,2.5*cm,1.8*cm,2.5*cm], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#15152a")),
        ("TEXTCOLOR",(0,0),(-1,0),colors.HexColor("#818cf8")),
        ("FONTSIZE",(0,0),(-1,0),7), ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor("#0f0f1a"),colors.HexColor("#07070f")]),
        ("TEXTCOLOR",(0,1),(-1,-1),colors.HexColor("#e2e2f0")),
        ("TEXTCOLOR",(3,1),(3,-1),colors.HexColor("#22c55e")),
        ("GRID",(0,0),(-1,-1),.3,colors.HexColor("#15152a")),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("TOPPADDING",(0,0),(-1,-1),4), ("BOTTOMPADDING",(0,0),(-1,-1),4),
    ]))
    story.append(t)
    story.append(Spacer(1, 14))
    story.append(Paragraph(
        f"Fonte: Portal Nacional de Contratações Públicas (PNCP) · pncp.gov.br · {date.today().strftime('%d/%m/%Y')}",
        sub_s
    ))

    doc.build(story)
    buf.seek(0)
    fname = f"pncp_licitacoes_{uf or 'BR'}_{date.today()}.pdf"
    return StreamingResponse(buf, media_type="application/pdf",
                             headers={"Content-Disposition": f"attachment; filename={fname}"})
