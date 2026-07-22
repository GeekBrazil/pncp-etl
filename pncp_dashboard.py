"""
PNCP Control Dashboard — FastAPI + SSE + PDF
Porta 8790
"""
import asyncio, json, os, threading, time, io
import queue as _queue
from datetime import date, timedelta
from typing import Optional
import psycopg2, psycopg2.extras
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials, APIKeyHeader
from sse_starlette.sse import EventSourceResponse
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

app = FastAPI(title="PNCP Control")

# Variáveis de ambiente para segurança
PNCP_API_KEY = os.environ.get("PNCP_API_KEY", "dev_key")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "dev_secret")

# Autenticações
security_basic = HTTPBasic(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_admin(credentials: Optional[HTTPBasicCredentials] = Depends(security_basic)):
    expected_username = "allan"
    expected_password = ADMIN_SECRET
    if not credentials or credentials.username != expected_username or credentials.password != expected_password:
        raise HTTPException(
            status_code=401,
            detail="Não autorizado",
            headers={"WWW-Authenticate": "Basic"},
        )

def verify_api_key_or_admin(
    api_key: Optional[str] = Depends(api_key_header),
    credentials: Optional[HTTPBasicCredentials] = Depends(security_basic)
):
    if api_key and api_key == PNCP_API_KEY:
        return
    if credentials:
        expected_username = "allan"
        expected_password = ADMIN_SECRET
        if credentials.username == expected_username and credentials.password == expected_password:
            return
    raise HTTPException(
        status_code=401,
        detail="Acesso não autorizado. Forneça uma API Key válida ou credenciais de administrador.",
        headers={"WWW-Authenticate": "Basic"},
    )

# String de conexão via variável de ambiente — nunca hardcoded (era "core-postgres"/"allan/Alcachofra",
# apontando pro servidor antigo que foi vendido).
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
UFS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]
# Mesma lista completa do etl.py — antes estava incompleta aqui (só 9 das 19 modalidades reais do PNCP).
MODALIDADES = {
    1: "Leilão Eletrônico", 2: "Diálogo Competitivo", 3: "Concurso",
    4: "Concorrência Eletrônica", 5: "Concorrência Presencial",
    6: "Pregão Eletrônico", 7: "Pregão Presencial",
    8: "Dispensa", 9: "Inexigibilidade", 10: "Manifestação de Interesse",
    11: "Pré-qualificação", 12: "Credenciamento", 13: "Leilão Presencial",
    14: "Inaplicabilidade", 15: "Chamada Pública",
    16: "Concorrência Eletrônica Internacional", 17: "Concorrência Presencial Internacional",
    18: "Pregão Eletrônico Internacional", 19: "Pregão Presencial Internacional",
}

_etl_queue: Optional[_queue.Queue] = None
_etl_task: Optional[threading.Thread] = None

def db():
    return psycopg2.connect(DATABASE_URL)

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

/* VIEW TABS (Licitações / Score / Alertas) */
.view-tabs{display:flex;gap:.35rem;flex-shrink:0}
.view-tab{
  padding:.42rem .95rem;border-radius:9px 9px 0 0;font-family:'Syne',sans-serif;
  font-size:.78rem;font-weight:700;color:var(--muted);background:transparent;
  border:1px solid transparent;border-bottom:none;cursor:pointer;transition:all .15s
}
.view-tab:hover{color:var(--text)}
.view-tab.active{color:var(--accent2);background:var(--bg2);border-color:var(--border)}
.view{flex:1;display:flex;flex-direction:column;overflow:hidden;gap:.65rem;min-height:0}
.subhead{font-size:.68rem;color:var(--muted);flex-shrink:0;margin:-.2rem 0 .1rem}
/* barra de receita per capita (Score) */
.score-bar-wrap{display:inline-block;width:64px;height:6px;background:var(--bg3);border-radius:3px;overflow:hidden;vertical-align:middle;margin-left:.45rem}
.score-bar{display:block;height:100%;background:linear-gradient(90deg,var(--accent),var(--green));border-radius:3px}
.ro-badge{font-size:.58rem;background:rgba(34,197,94,.15);color:var(--green);padding:.12rem .5rem;border-radius:10px;margin-left:.55rem;letter-spacing:.04em;text-transform:uppercase}

/* MODO READ-ONLY (/publico) — some com controles de admin */
body.readonly .admin-only{display:none!important}
body.readonly .left-panel{display:none}
body.readonly .layout{grid-template-columns:1fr}

@media(max-width:800px){
  .layout{grid-template-columns:1fr;grid-template-rows:auto 1fr}
  html,body{height:auto;overflow:auto}
  .left-panel{max-height:280px}
}
</style>
</head>
<body class="__BODYCLASS__">
<script>window.READONLY = __READONLY__;</script>
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

      <div class="row-btns" style="margin-top:.6rem">
        <button class="btn btn-primary" id="btn-enrich" onclick="runEnrich()">🏢 Enriquecer CNPJs pendentes</button>
      </div>
      <div id="enrich-log-box" style="max-height:140px;overflow-y:auto;font-size:.68rem;margin-top:.3rem"></div>
    </div>

    <div>
      <div class="panel-title">📋 Histórico</div>
      <div class="hist-card" id="hist-list"><span style="color:var(--muted)">Carregando...</span></div>
    </div>
  </div>

  <!-- RIGHT: Views (Licitações / Score Territorial / Alertas) -->
  <div class="right-panel">

    <div class="view-tabs">
      <button class="view-tab active" data-view="licitacoes" onclick="switchView('licitacoes')">📊 Licitações</button>
      <button class="view-tab" data-view="score" onclick="switchView('score')">🏙 Score Territorial</button>
      <button class="view-tab" data-view="alertas" onclick="switchView('alertas')">🚨 Alertas</button>
    </div>

    <!-- VIEW: Licitações -->
    <div id="view-licitacoes" class="view">
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
          <button class="btn-sm btn-sm-green admin-only" onclick="gerarPDF()">📄 PDF</button>
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
    </div><!-- /view-licitacoes -->

    <!-- VIEW: Score Territorial -->
    <div id="view-score" class="view" style="display:none">
      <div class="kpis-bar">
        <div class="kpi"><div class="label">Municípios</div><div class="val" id="sc-count">—</div></div>
        <div class="kpi"><div class="label">Média / hab</div><div class="val yellow" id="sc-media">—</div></div>
        <div class="kpi"><div class="label">Maior / hab</div><div class="val green" id="sc-max">—</div></div>
      </div>
      <div class="table-card">
        <div class="table-header">
          <h2>🏙 Score Territorial</h2>
          <div class="search-bar">
            <input type="text" id="sc-search" placeholder="Buscar município..." oninput="renderScore()">
            <select id="sc-uf" onchange="loadScore()"><option value="">Todos estados</option></select>
            <span class="count-badge" id="sc-count-badge"></span>
          </div>
          <div class="action-btns">
            <button class="btn-sm btn-sm-outline" onclick="loadScore()" title="Atualizar">🔄</button>
          </div>
        </div>
        <div class="subhead">Receita municipal realizada (SICONFI) ÷ população (IBGE) — indicador de saúde fiscal / capacidade local.</div>
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>Município</th>
              <th>UF</th>
              <th style="text-align:right">População</th>
              <th style="text-align:right">Receita Realizada</th>
              <th style="text-align:right">Receita / hab</th>
            </tr></thead>
            <tbody id="sc-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- VIEW: Alertas (Dispensa/Inexigibilidade de alto valor) -->
    <div id="view-alertas" class="view" style="display:none">
      <div class="kpis-bar">
        <div class="kpi"><div class="label">Contratações</div><div class="val" id="al-count">—</div></div>
        <div class="kpi"><div class="label">Valor Total</div><div class="val yellow" id="al-valor">—</div></div>
        <div class="kpi"><div class="label">Maior valor</div><div class="val green" id="al-max">—</div></div>
      </div>
      <div class="table-card">
        <div class="table-header">
          <h2>🚨 Alertas de Transparência</h2>
          <div class="search-bar">
            <input type="text" id="al-search" placeholder="Buscar órgão, objeto, município..." oninput="renderAlertas()">
            <select id="al-uf" onchange="loadAlertas()"><option value="">Todos estados</option></select>
            <select id="al-min" onchange="loadAlertas()">
              <option value="100000">≥ R$ 100 mil</option>
              <option value="500000">≥ R$ 500 mil</option>
              <option value="1000000" selected>≥ R$ 1 mi</option>
              <option value="5000000">≥ R$ 5 mi</option>
              <option value="10000000">≥ R$ 10 mi</option>
            </select>
            <span class="count-badge" id="al-count-badge"></span>
          </div>
          <div class="action-btns">
            <button class="btn-sm btn-sm-outline" onclick="loadAlertas()" title="Atualizar">🔄</button>
          </div>
        </div>
        <div class="subhead">Dispensa e Inexigibilidade de alto valor — compras que, em geral, deveriam passar por disputa aberta. Sinal clássico de baixa concorrência.</div>
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>Órgão</th>
              <th>Município</th>
              <th>UF</th>
              <th>Objeto</th>
              <th style="text-align:right">Valor</th>
              <th>Modalidade</th>
              <th>Publicação</th>
              <th>PNCP</th>
            </tr></thead>
            <tbody id="al-tbody"></tbody>
          </table>
        </div>
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

  loadKPIs(); loadDados();
  if (!window.READONLY) loadHist();
  initExtraViews();
  if (window.READONLY) document.querySelector('.logo').insertAdjacentHTML('beforeend', '<span class="ro-badge">read-only</span>');
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

let enrichEvtSource = null;
async function runEnrich() {
  document.getElementById('btn-enrich').disabled = true;
  const log = document.getElementById('enrich-log-box');
  log.innerHTML = '';

  await fetch('/empresas/enriquecer', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({})
  });

  if (enrichEvtSource) enrichEvtSource.close();
  enrichEvtSource = new EventSource('/empresas/enriquecer/stream');
  enrichEvtSource.onmessage = e => {
    log.innerHTML += `<div>${e.data}</div>`;
    log.scrollTop = log.scrollHeight;
    if (e.data.startsWith('DONE')) {
      enrichEvtSource.close(); enrichEvtSource = null;
      document.getElementById('btn-enrich').disabled = false;
    }
  };
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

/* ─── Views: Score Territorial + Alertas ─────────────────────────────── */
function brlShort(v){
  v = Number(v) || 0;
  if (v >= 1e9) return 'R$ ' + (v/1e9).toFixed(1).replace('.',',') + ' Bi';
  if (v >= 1e6) return 'R$ ' + (v/1e6).toFixed(1).replace('.',',') + ' M';
  return 'R$ ' + Math.round(v).toLocaleString('pt-BR');
}
const _brl = v => v ? 'R$ ' + Number(v).toLocaleString('pt-BR', {maximumFractionDigits:0}) : '—';
const _esc = s => String(s == null ? '—' : s).replace(/</g, '&lt;');

let scoreLoaded = false, alertasLoaded = false;
function switchView(name) {
  document.querySelectorAll('.view-tab').forEach(t => t.classList.toggle('active', t.dataset.view === name));
  document.querySelectorAll('.view').forEach(v => v.style.display = (v.id === 'view-' + name) ? 'flex' : 'none');
  if (name === 'score'   && !scoreLoaded)   loadScore();
  if (name === 'alertas' && !alertasLoaded) loadAlertas();
}

function initExtraViews() {
  const opts = '<option value="">Todos estados</option>' + UFS.map(u => `<option>${u}</option>`).join('');
  document.getElementById('sc-uf').innerHTML = opts;
  document.getElementById('al-uf').innerHTML = opts;
}

let scoreRows = [];
async function loadScore() {
  scoreLoaded = true;
  const uf = document.getElementById('sc-uf').value;
  const r = await fetch('/score-municipios?limit=500' + (uf ? '&uf=' + uf : ''));
  scoreRows = await r.json();
  const pcs = scoreRows.map(d => parseFloat(d.receita_per_capita) || 0).filter(v => v > 0);
  const media = pcs.length ? pcs.reduce((a,b) => a+b, 0) / pcs.length : 0;
  document.getElementById('sc-count').textContent = scoreRows.length.toLocaleString('pt-BR');
  document.getElementById('sc-media').textContent = brlShort(media);
  document.getElementById('sc-max').textContent   = brlShort(pcs.length ? Math.max(...pcs) : 0);
  renderScore();
}

function renderScore() {
  const q = document.getElementById('sc-search').value.toLowerCase();
  const rows = scoreRows.filter(d => !q || (d.municipio_nome || '').toLowerCase().includes(q));
  const maxPc = Math.max(1, ...rows.map(d => parseFloat(d.receita_per_capita) || 0));
  document.getElementById('sc-count-badge').textContent = rows.length.toLocaleString('pt-BR') + ' municípios';
  document.getElementById('sc-tbody').innerHTML = rows.map(d => {
    const pc = parseFloat(d.receita_per_capita) || 0;
    const w = Math.round(pc / maxPc * 100);
    return `<tr>
      <td class="td-muted">${_esc(d.municipio_nome)}</td>
      <td class="td-muted">${_esc(d.uf)}</td>
      <td class="td-muted" style="text-align:right">${(d.populacao || 0).toLocaleString('pt-BR')}</td>
      <td class="td-val">${_brl(d.receita_realizada)}</td>
      <td style="text-align:right;white-space:nowrap"><span style="color:var(--accent2);font-weight:600">${_brl(pc)}</span><span class="score-bar-wrap"><span class="score-bar" style="width:${w}%"></span></span></td>
    </tr>`;
  }).join('');
}

let alertasRows = [];
async function loadAlertas() {
  alertasLoaded = true;
  const uf  = document.getElementById('al-uf').value;
  const min = document.getElementById('al-min').value;
  const r = await fetch(`/alertas/alto-valor-dispensa?valor_min=${min}&limit=300` + (uf ? '&uf=' + uf : ''));
  alertasRows = await r.json();
  const vals = alertasRows.map(d => parseFloat(d.valor_estimado) || 0);
  document.getElementById('al-count').textContent = alertasRows.length.toLocaleString('pt-BR');
  document.getElementById('al-valor').textContent = brlShort(vals.reduce((a,b) => a+b, 0));
  document.getElementById('al-max').textContent   = brlShort(vals.length ? Math.max(...vals) : 0);
  renderAlertas();
}

function renderAlertas() {
  const q = document.getElementById('al-search').value.toLowerCase();
  const rows = alertasRows.filter(d => !q || [d.orgao_nome, d.objeto, d.municipio_nome].join(' ').toLowerCase().includes(q));
  document.getElementById('al-count-badge').textContent = rows.length.toLocaleString('pt-BR') + ' contratações';
  document.getElementById('al-tbody').innerHTML = rows.map(d => `
    <tr>
      <td style="max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.73rem;color:var(--muted)" title="${_esc(d.orgao_nome)}">${_esc((d.orgao_nome || '').slice(0,36))}</td>
      <td class="td-muted">${_esc(d.municipio_nome)}</td>
      <td class="td-muted">${_esc(d.uf)}</td>
      <td class="td-obj" title="${_esc(d.objeto)}">${_esc((d.objeto || '').slice(0,60))}</td>
      <td class="td-val">${_brl(d.valor_estimado)}</td>
      <td class="td-muted" style="font-size:.72rem">${_esc(d.modalidade_nome)}</td>
      <td class="td-date">${d.data_publicacao || '—'}</td>
      <td class="td-link">${d.url_pncp ? `<a href="${d.url_pncp}" target="_blank" rel="noopener">↗</a>` : '—'}</td>
    </tr>`).join('');
}
</script>
</body>
</html>"""

# ─── Endpoints ────────────────────────────────────────────────────────────────
def render_dashboard(readonly: bool) -> str:
    """Mesmo template servido em dois modos: admin (/) e read-only (/publico).
    No modo read-only o CSS `body.readonly` esconde todos os controles de admin
    (ETL, enriquecer CNPJ, PDF, histórico) — sobram só as views de dados."""
    return (HTML
            .replace("__BODYCLASS__", "readonly" if readonly else "")
            .replace("__READONLY__", "true" if readonly else "false"))

@app.get("/", response_class=HTMLResponse, dependencies=[Depends(verify_admin)])
async def dashboard(): return render_dashboard(False)

@app.get("/publico", response_class=HTMLResponse, dependencies=[Depends(verify_api_key_or_admin)])
async def dashboard_publico():
    """Painel read-only (sem botões de admin). Continua atrás de autenticação
    (X-API-Key ou Basic Auth) — só remove da UI as ações que mutam dados."""
    return render_dashboard(True)

@app.get("/kpis", dependencies=[Depends(verify_api_key_or_admin)])
async def kpis():
    # valor_estimado > 500mi é quase sempre erro de digitação na fonte (PNCP) —
    # ex: sentinela 9999999999999.99 — exclui só da SOMA pra não distorcer o KPI.
    rows = query("""
        SELECT COUNT(*) AS total,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor,
               COUNT(DISTINCT uf) AS estados,
               COUNT(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas
        FROM licitacoes
    """)
    return rows[0] if rows else {}

@app.get("/dados", dependencies=[Depends(verify_api_key_or_admin)])
async def dados(limit: int = 5000, uf: str = None, modalidade: int = None):
    sql = "SELECT * FROM licitacoes WHERE 1=1"
    params = []
    if uf: sql += " AND uf=%s"; params.append(uf)
    if modalidade: sql += " AND modalidade_id=%s"; params.append(modalidade)
    sql += " ORDER BY valor_estimado DESC LIMIT %s"; params.append(limit)
    return query(sql, params)

@app.get("/etl/status", dependencies=[Depends(verify_admin)])
async def etl_status():
    return query("SELECT * FROM etl_log ORDER BY id DESC LIMIT 20")

# ─── ETL assíncrono com SSE (thread-safe via queue.Queue) ─────────────────────
@app.post("/etl/run", dependencies=[Depends(verify_admin)])
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

@app.get("/etl/stream", dependencies=[Depends(verify_admin)])
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

@app.post("/etl/stop", dependencies=[Depends(verify_admin)])
async def etl_stop():
    global _etl_queue
    if _etl_queue:
        _etl_queue.put("DONE Parado")
    return {"ok": True}

# ─── Enriquecimento de CNPJ (Receita Federal via BrasilAPI) ───────────────────
_enrich_queue: Optional[_queue.Queue] = None
_enrich_task: Optional[threading.Thread] = None

@app.get("/empresas/{cnpj}", dependencies=[Depends(verify_api_key_or_admin)])
async def empresa_detalhe(cnpj: str):
    empresas = query("SELECT * FROM empresas WHERE cnpj = %s", (cnpj,))
    if not empresas:
        return {"encontrado": False}
    socios_rows = query("SELECT nome_socio, qualificacao, data_entrada_sociedade, faixa_etaria FROM socios WHERE cnpj = %s", (cnpj,))
    return {"encontrado": True, "empresa": empresas[0], "socios": socios_rows}

@app.get("/empresas", dependencies=[Depends(verify_api_key_or_admin)])
async def empresas_busca(nome_socio: str = None, limit: int = 50):
    if nome_socio:
        rows = query(
            """SELECT DISTINCT e.cnpj, e.razao_social, e.municipio, e.uf, s.nome_socio, s.qualificacao
               FROM socios s JOIN empresas e ON e.cnpj = s.cnpj
               WHERE s.nome_socio ILIKE %s
               LIMIT %s""",
            (f"%{nome_socio}%", limit),
        )
        return rows
    return query("SELECT cnpj, razao_social, situacao_cadastral, municipio, uf FROM empresas ORDER BY atualizado_em DESC LIMIT %s", (limit,))

@app.post("/empresas/enriquecer", dependencies=[Depends(verify_admin)])
async def empresas_enriquecer(request: Request):
    global _enrich_task, _enrich_queue
    body = await request.json() if await request.body() else {}
    _enrich_queue = _queue.Queue()
    _enrich_task = threading.Thread(target=_run_enrich_thread, args=(_enrich_queue, body.get("limite")), daemon=True)
    _enrich_task.start()
    return {"ok": True}

def _run_enrich_thread(q: _queue.Queue, limite: Optional[int]):
    import sys
    sys.path.insert(0, "/app")
    from cnpj_enrich import buscar_cnpj, salvar_empresa, get_conn

    conn = get_conn()
    with conn.cursor() as cur:
        sql = """SELECT DISTINCT orgao_cnpj FROM licitacoes
                 WHERE orgao_cnpj IS NOT NULL AND orgao_cnpj != ''
                   AND orgao_cnpj NOT IN (SELECT cnpj FROM empresas)"""
        if limite:
            sql += f" LIMIT {int(limite)}"
        cur.execute(sql)
        pendentes = [row[0] for row in cur.fetchall()]

    q.put(f"[INFO] {len(pendentes)} CNPJ(s) pendente(s)")
    ok = erros = 0
    for i, cnpj in enumerate(pendentes, 1):
        dados = buscar_cnpj(cnpj)
        if dados:
            try:
                salvar_empresa(conn, dados)
                ok += 1
                q.put(f"[{i}/{len(pendentes)}] {cnpj} → {dados.get('razao_social', '?')}")
            except Exception as e:
                erros += 1
                conn.rollback()
                q.put(f"[{i}/{len(pendentes)}] {cnpj} → erro ao salvar: {e}")
        else:
            erros += 1
            q.put(f"[{i}/{len(pendentes)}] {cnpj} → não encontrado")
        import time as _time
        _time.sleep(0.4)

    conn.close()
    q.put(f"DONE Concluído: {ok} ok, {erros} erros de {len(pendentes)} pendentes.")

@app.get("/empresas/enriquecer/stream", dependencies=[Depends(verify_admin)])
async def empresas_enriquecer_stream():
    async def generator():
        global _enrich_queue
        if not _enrich_queue:
            yield {"data": "Nenhum enriquecimento em execução"}
            return
        while True:
            try:
                msg = _enrich_queue.get_nowait()
                yield {"data": msg}
                if msg.startswith("DONE"):
                    break
            except _queue.Empty:
                await asyncio.sleep(0.2)
    return EventSourceResponse(generator())

# ─── Imóveis da União ──────────────────────────────────────────────────────────
@app.get("/imoveis-uniao", dependencies=[Depends(verify_api_key_or_admin)])
async def imoveis_uniao_lista(
    municipio: str = None, uf: str = None, organizacao: str = None,
    valor_min: float = None, valor_max: float = None, limit: int = 100,
):
    sql = "SELECT * FROM imoveis_uniao WHERE 1=1"
    params = []
    if municipio:
        sql += " AND municipio ILIKE %s"; params.append(f"%{municipio}%")
    if uf:
        sql += " AND uf = %s"; params.append(uf)
    if organizacao:
        sql += " AND fonte_organizacao ILIKE %s"; params.append(f"%{organizacao}%")
    if valor_min is not None:
        sql += " AND valor_imovel >= %s"; params.append(valor_min)
    if valor_max is not None:
        sql += " AND valor_imovel <= %s"; params.append(valor_max)
    sql += " ORDER BY valor_imovel DESC NULLS LAST LIMIT %s"; params.append(limit)
    return query(sql, params)

@app.get("/imoveis-uniao/stats", dependencies=[Depends(verify_api_key_or_admin)])
async def imoveis_uniao_stats():
    geral = query("SELECT count(*) AS total, COALESCE(sum(valor_imovel),0) AS valor_total FROM imoveis_uniao")
    por_org = query(
        """SELECT fonte_organizacao, count(*) AS total, COALESCE(sum(valor_imovel),0) AS valor_total
           FROM imoveis_uniao GROUP BY fonte_organizacao ORDER BY total DESC LIMIT 30"""
    )
    return {"total": geral[0]["total"], "valor_total": float(geral[0]["valor_total"]), "por_organizacao": por_org}

@app.get("/alertas/alto-valor-dispensa", dependencies=[Depends(verify_api_key_or_admin)])
async def alertas_alto_valor_dispensa(valor_min: float = 100000, valor_max: float = 500000000, uf: str = None, limit: int = 200):
    """Contratações via Dispensa (8) ou Inexigibilidade (9) acima de um valor —
    sinal clássico de transparência: valores altos deveriam, em geral, passar
    por disputa aberta (pregão/concorrência), não por dispensa/inexigibilidade.
    valor_max evita expor erros de digitação da fonte (PNCP) como se fossem reais —
    passe um valor bem alto explicitamente se quiser inspecionar os outliers brutos."""
    sql = """SELECT pncp_id, orgao_nome, orgao_cnpj, municipio_nome, uf, objeto,
                     valor_estimado, modalidade_nome, data_publicacao, url_pncp
              FROM licitacoes
              WHERE modalidade_id IN (8, 9) AND valor_estimado >= %s AND valor_estimado <= %s"""
    params = [valor_min, valor_max]
    if uf:
        sql += " AND uf = %s"; params.append(uf)
    sql += " ORDER BY valor_estimado DESC LIMIT %s"; params.append(limit)
    return query(sql, params)

@app.get("/score-municipios", dependencies=[Depends(verify_api_key_or_admin)])
async def score_municipios_lista(
    uf: str = None, municipio: str = None,
    per_capita_min: float = None, per_capita_max: float = None,
    limit: int = 100,
):
    sql = "SELECT * FROM score_municipios WHERE 1=1"
    params = []
    if uf:
        sql += " AND uf = %s"; params.append(uf)
    if municipio:
        sql += " AND municipio_nome ILIKE %s"; params.append(f"%{municipio}%")
    if per_capita_min is not None:
        sql += " AND receita_per_capita >= %s"; params.append(per_capita_min)
    if per_capita_max is not None:
        sql += " AND receita_per_capita <= %s"; params.append(per_capita_max)
    sql += " ORDER BY receita_per_capita DESC NULLS LAST LIMIT %s"; params.append(limit)
    return query(sql, params)

# ─── Ferramentas públicas (allancandido.com/ferramentas) ─────────────────────
import re as _re
from cnpj_enrich import buscar_cnpj as _buscar_cnpj_api, salvar_empresa as _salvar_empresa

@app.get("/cnpj/{cnpj}", dependencies=[Depends(verify_api_key_or_admin)])
async def consulta_cnpj(cnpj: str):
    """Consulta de CNPJ cache-first: banco próprio → BrasilAPI (e salva no
    cache, engordando a base a cada consulta de usuário)."""
    digitos = _re.sub(r"\D", "", cnpj)
    if len(digitos) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve ter 14 dígitos")

    empresa = query("SELECT * FROM empresas WHERE cnpj = %s", (digitos,))
    if not empresa:
        dados = _buscar_cnpj_api(digitos)
        if not dados:
            raise HTTPException(status_code=404, detail="CNPJ não encontrado")
        conn = db()
        try:
            _salvar_empresa(conn, dados)
            conn.commit()
        finally:
            conn.close()
        empresa = query("SELECT * FROM empresas WHERE cnpj = %s", (digitos,))
    socios = query("SELECT nome_socio, qualificacao, data_entrada_sociedade FROM socios WHERE cnpj = %s ORDER BY nome_socio", (digitos,))
    e = dict(empresa[0]); e.pop("raw_json", None)
    # cruzamento investigativo: a empresa está sancionada? (CEIS/CNEP/CEPIM)
    sancoes = query(
        """SELECT cadastro, tipo_sancao, orgao_sancionador, fonte, data_inicio, data_fim, link_publicacao
           FROM sancoes WHERE cnpj = %s ORDER BY data_fim DESC NULLS LAST""",
        (digitos,))
    ativas = [s for s in sancoes if not s["data_fim"] or str(s["data_fim"]) >= date.today().isoformat()]
    # gasto executado: quanto de recurso federal a empresa recebeu (Portal da Transparência)
    # — cruzamento investigativo com as sanções. Resiliente: nunca quebra a consulta.
    try:
        from transparencia_etl import recursos_por_cnpj as _recursos_cnpj
        recursos = _recursos_cnpj(digitos, buscar_api=False)  # só cache: não trava a consulta
    except Exception:
        recursos = None
    return {"empresa": e, "socios": socios,
            "sancoes": sancoes,
            "sancionada": len(ativas) > 0,
            "sancoes_ativas": len(ativas),
            "recursos_federais": recursos}

@app.get("/sancoes/stats", dependencies=[Depends(verify_api_key_or_admin)])
async def sancoes_stats():
    """Números da base de sanções (CEIS/CNEP/CEPIM) pra prova/contadores."""
    por_cad = query("SELECT cadastro, count(*) AS total FROM sancoes GROUP BY cadastro ORDER BY 1")
    tot = query("""SELECT count(*) AS total,
                          count(*) FILTER (WHERE cnpj IS NOT NULL) AS empresas,
                          count(*) FILTER (WHERE data_fim IS NULL OR data_fim >= CURRENT_DATE) AS vigentes
                   FROM sancoes""")[0]
    return {"por_cadastro": por_cad, **tot}

@app.get("/sancoes/por-cnpj/{cnpj}", dependencies=[Depends(verify_api_key_or_admin)])
async def sancoes_por_cnpj(cnpj: str):
    """Consulta de risco: uma empresa está sancionada? Retorna sanções + flag ativa."""
    digitos = _re.sub(r"\D", "", cnpj)
    if len(digitos) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve ter 14 dígitos")
    sancoes = query(
        """SELECT cadastro, nome, tipo_sancao, orgao_sancionador, fonte,
                  data_inicio, data_fim, link_publicacao
           FROM sancoes WHERE cnpj = %s ORDER BY data_fim DESC NULLS LAST""",
        (digitos,))
    hoje = date.today().isoformat()
    ativas = [s for s in sancoes if not s["data_fim"] or str(s["data_fim"]) >= hoje]
    return {"cnpj": digitos, "sancionada": len(ativas) > 0,
            "sancoes_ativas": len(ativas), "sancoes": sancoes}

# ─── Portal da Transparência (CGU) — gasto executado (5º pilar) ───────────────
@app.get("/transparencia/recursos-cnpj/{cnpj}", dependencies=[Depends(verify_api_key_or_admin)])
async def transparencia_recursos_cnpj(cnpj: str, force: bool = False):
    """Recursos federais recebidos por um CNPJ (gasto executado da União),
    cache-first. Cruza com sanções: 'empresa sancionada recebendo da União'."""
    from transparencia_etl import recursos_por_cnpj
    r = recursos_por_cnpj(cnpj, force=force)
    if r is None:
        raise HTTPException(status_code=400, detail="CNPJ inválido (14 dígitos) ou chave da Transparência ausente.")
    return r

@app.get("/transparencia/bolsa-familia/{ibge}", dependencies=[Depends(verify_api_key_or_admin)])
async def transparencia_bolsa_familia(ibge: str, meses: int = 6):
    """Novo Bolsa Família por município (gasto social federal executado) — sinal
    territorial pro raio-X municipal. Cache-first."""
    from transparencia_etl import bolsa_familia_municipio
    r = bolsa_familia_municipio(ibge, meses=min(max(meses, 1), 12))
    if r is None:
        raise HTTPException(status_code=400, detail="Código IBGE inválido.")
    return r

# ─── Mercado imobiliário: avaliador (AVM) + preço/m² por bairro ───────────────
@app.get("/avaliador", dependencies=[Depends(verify_api_key_or_admin)])
async def avaliador(cidade: str, tipo: str = None, area: float = None, bairro: str = None):
    """Estimativa de valor de referência por comparáveis (não é avaliação NBR 14653)."""
    from avm import avaliar as _avaliar
    conn = db()
    try:
        return _avaliar(conn, cidade, tipo=tipo, area=area, bairro=bairro)
    finally:
        conn.close()

# Faixa de plausibilidade do preço/m² (R$) — corta extrações erradas de área
# (ex: R$24/m² ou R$19.000/m² vindos de m² mal lido no anúncio).
PRECO_M2_MIN, PRECO_M2_MAX = 200, 70000

@app.get("/precos-mercado", dependencies=[Depends(verify_api_key_or_admin)])
async def precos_mercado(cidade: str, tipo: str = None, limit: int = 200):
    """Preço/m² e preço mediano por bairro (a partir dos anúncios coletados)."""
    sql = """SELECT bairro, tipo, count(*) AS n,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY preco_m2) AS preco_m2_med,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY preco) AS preco_med
             FROM imoveis_mercado
             WHERE preco_m2 BETWEEN %s AND %s AND cidade ILIKE %s"""
    params = [PRECO_M2_MIN, PRECO_M2_MAX, cidade]
    if tipo:
        sql += " AND tipo = %s"; params.append(tipo)
    sql += " GROUP BY bairro, tipo HAVING count(*) >= 2 ORDER BY preco_m2_med DESC LIMIT %s"
    params.append(min(limit, 500))
    return query(sql, params)

@app.get("/mercado/anuncios", dependencies=[Depends(verify_api_key_or_admin)])
async def mercado_anuncios(cidade: str = None, bairro: str = None, tipo: str = None,
                           imobiliaria_id: int = None, limit: int = 60):
    """Anúncios individuais (o detalhe atrás das medianas) — filtráveis por
    bairro/tipo (drill-down do preço/m²) ou por imobiliária (vitrine)."""
    sql = """SELECT a.id, a.preco, a.area, a.preco_m2, a.quartos, a.tipo, a.finalidade,
                    a.bairro, a.cidade, a.uf, a.url, a.coletado_em,
                    i.id AS imobiliaria_id, i.nome AS imobiliaria, i.site AS imobiliaria_site
             FROM imoveis_mercado a LEFT JOIN imobiliarias i ON i.id = a.imobiliaria_id
             WHERE a.preco IS NOT NULL
               AND (a.preco_m2 IS NULL OR a.preco_m2 BETWEEN %s AND %s)"""
    params = [PRECO_M2_MIN, PRECO_M2_MAX]
    if cidade:
        sql += " AND a.cidade ILIKE %s"; params.append(cidade)
    if bairro:
        sql += " AND a.bairro ILIKE %s"; params.append(bairro)
    if tipo:
        sql += " AND a.tipo = %s"; params.append(tipo)
    if imobiliaria_id:
        sql += " AND a.imobiliaria_id = %s"; params.append(imobiliaria_id)
    sql += " ORDER BY a.preco_m2 IS NULL, a.coletado_em DESC LIMIT %s"
    params.append(min(limit, 200))
    return query(sql, params)

@app.get("/imobiliarias/diretorio", dependencies=[Depends(verify_api_key_or_admin)])
async def imobiliarias_diretorio(cidade: str = None):
    """Imobiliárias com site monitorado + quantos anúncios ativos de cada uma."""
    sql = """SELECT i.id, i.nome, i.site, i.telefone, i.cidade, i.uf,
                    count(a.id) AS anuncios,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY a.preco_m2)
                        FILTER (WHERE a.preco_m2 BETWEEN %s AND %s) AS preco_m2_med
             FROM imobiliarias i LEFT JOIN imoveis_mercado a ON a.imobiliaria_id = i.id
             WHERE i.coletavel"""
    params = [PRECO_M2_MIN, PRECO_M2_MAX]
    if cidade:
        sql += " AND i.cidade ILIKE %s"; params.append(cidade)
    sql += " GROUP BY i.id, i.nome, i.site, i.telefone, i.cidade, i.uf ORDER BY anuncios DESC"
    return query(sql, params)

@app.get("/mercado/opcoes", dependencies=[Depends(verify_api_key_or_admin)])
async def mercado_opcoes(cidade: str = None):
    """Cidades/bairros/tipos disponíveis na base de mercado (pros filtros da UI).
    `regioes` une cidades com anúncios coletados e cidades com imobiliárias
    achadas via CNPJ — o seletor da página mostra todas."""
    cidades = [r["cidade"] for r in query(
        "SELECT cidade, count(*) n FROM imoveis_mercado WHERE cidade IS NOT NULL GROUP BY cidade ORDER BY n DESC")]
    regioes = query(
        """SELECT cidade, max(uf) AS uf, sum(anuncios)::int AS anuncios, sum(leads)::int AS leads
           FROM (
             SELECT cidade, uf, count(*) AS anuncios, 0 AS leads
             FROM imoveis_mercado WHERE cidade IS NOT NULL GROUP BY cidade, uf
             UNION ALL
             SELECT l.cidade_alvo, l.uf, 0, count(*)
             FROM leads_imobiliarias l JOIN empresas e ON e.cnpj = l.cnpj
             WHERE e.situacao_cadastral = '02' GROUP BY l.cidade_alvo, l.uf
           ) t GROUP BY cidade ORDER BY sum(anuncios) DESC, sum(leads) DESC""")
    bairros, tipos = [], []
    if cidade:
        bairros = [r["bairro"] for r in query(
            "SELECT bairro, count(*) n FROM imoveis_mercado WHERE cidade ILIKE %s AND bairro IS NOT NULL GROUP BY bairro ORDER BY n DESC", (cidade,))]
        tipos = [r["tipo"] for r in query(
            "SELECT tipo, count(*) n FROM imoveis_mercado WHERE cidade ILIKE %s AND tipo IS NOT NULL GROUP BY tipo ORDER BY n DESC", (cidade,))]
    return {"cidades": cidades, "regioes": regioes, "bairros": bairros, "tipos": tipos}

@app.get("/leads/imobiliarias", dependencies=[Depends(verify_api_key_or_admin)])
async def leads_imobiliarias(cidade: str = None, limit: int = 100):
    """Imobiliárias achadas via CNPJ (CNAE de corretagem) — leads pro /pro."""
    sql = """SELECT e.cnpj, e.nome_fantasia, e.telefone, e.email, e.data_abertura,
                    l.cidade_alvo, l.uf, l.alertado, l.capturado_em
             FROM leads_imobiliarias l JOIN empresas e ON e.cnpj = l.cnpj
             WHERE e.situacao_cadastral = '02'"""
    params = []
    if cidade:
        sql += " AND l.cidade_alvo ILIKE %s"
        params.append(cidade)
    sql += " ORDER BY l.capturado_em DESC LIMIT %s"
    params.append(min(limit, 500))
    return query(sql, params)

@app.get("/leads/imobiliarias/stats", dependencies=[Depends(verify_api_key_or_admin)])
async def leads_imobiliarias_stats():
    """Contagem por cidade — pro seletor da UI."""
    return query(
        """SELECT l.cidade_alvo AS cidade, l.uf, count(*) AS total,
                  count(*) FILTER (WHERE NOT l.alertado) AS novos
           FROM leads_imobiliarias l JOIN empresas e ON e.cnpj = l.cnpj
           WHERE e.situacao_cadastral = '02'
           GROUP BY l.cidade_alvo, l.uf ORDER BY total DESC"""
    )

@app.get("/mercado-publico", dependencies=[Depends(verify_api_key_or_admin)])
async def mercado_publico(q: str, uf: str = None):
    """Quanto o governo compra de <termo>? Estatísticas + maiores compradores +
    oportunidades abertas, sobre a base completa de licitações (índice trigram)."""
    q = (q or "").strip()
    if len(q) < 3:
        raise HTTPException(status_code=400, detail="Termo deve ter pelo menos 3 caracteres")
    padrao = f"%{q}%"
    filtro_uf, params_uf = ("", []) if not uf else (" AND uf = %s", [uf])

    stats = query(f"""
        SELECT COUNT(*) AS total,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor,
               COUNT(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas,
               COUNT(DISTINCT municipio_ibge) AS municipios
        FROM licitacoes WHERE objeto ILIKE %s{filtro_uf}
    """, [padrao, *params_uf])[0]

    top_orgaos = query(f"""
        SELECT orgao_nome, uf, COUNT(*) AS total,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor
        FROM licitacoes WHERE objeto ILIKE %s{filtro_uf}
        GROUP BY orgao_nome, uf ORDER BY valor DESC NULLS LAST LIMIT 5
    """, [padrao, *params_uf])

    abertas = query(f"""
        SELECT pncp_id, orgao_nome, orgao_cnpj, municipio_nome, uf, objeto,
               valor_estimado, modalidade_nome, situacao,
               data_publicacao, data_encerramento, url_pncp
        FROM licitacoes
        WHERE objeto ILIKE %s{filtro_uf}
          AND data_encerramento >= CURRENT_DATE AND valor_estimado <= 500000000
        ORDER BY data_encerramento ASC LIMIT 8
    """, [padrao, *params_uf])

    # série mensal (últimos 12 meses) — trajetória do termo pra curiosidade/pesquisa
    serie = query(f"""
        SELECT to_char(date_trunc('month', data_publicacao), 'YYYY-MM') AS mes,
               COUNT(*) AS total,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor
        FROM licitacoes
        WHERE objeto ILIKE %s{filtro_uf}
          AND data_publicacao >= (date_trunc('month', CURRENT_DATE) - INTERVAL '11 months')
        GROUP BY 1 ORDER BY 1
    """, [padrao, *params_uf])

    # tendência: ignora o mês corrente (parcial) e compara 3 meses fechados vs 3 anteriores
    tot = [int(s["total"] or 0) for s in serie]
    mes_atual = date.today().strftime("%Y-%m")
    tot_fechados = tot[:-1] if serie and serie[-1]["mes"] == mes_atual else tot
    tendencia = None
    if len(tot_fechados) >= 6:
        rec, ant = sum(tot_fechados[-3:]), sum(tot_fechados[-6:-3]) or 0
        if ant == 0:
            tendencia = {"direcao": "alta", "variacao_pct": None} if rec else None
        else:
            v = round((rec - ant) / ant * 100)
            tendencia = {"direcao": "alta" if v >= 8 else "queda" if v <= -8 else "estavel",
                         "variacao_pct": v}

    return {"stats": stats, "top_orgaos": top_orgaos, "abertas": abertas,
            "serie_mensal": serie, "tendencia": tendencia}

# Objetos de leilão que são imóveis (e não veículos/sucata/máquinas)
LEILAO_IMOVEL_REGEX = r"imóve|imove|terreno|gleba|casa|apartamento|sala comercial|galpão|fazenda|sítio|chácara|prédio|edifício|área urbana|área rural"

@app.get("/leiloes-imoveis", dependencies=[Depends(verify_api_key_or_admin)])
async def leiloes_imoveis(uf: str = None, abertos: bool = True, limit: int = 50):
    """Leilões públicos (modalidades 1 e 13 do PNCP) cujo objeto é imóvel/terreno —
    oportunidade clássica pra investidor de leilão, direto da fonte oficial."""
    sql = """SELECT pncp_id, orgao_nome, orgao_cnpj, municipio_nome, uf, objeto,
                     valor_estimado, modalidade_nome, situacao,
                     data_publicacao, data_encerramento, url_pncp
              FROM licitacoes
              WHERE modalidade_id IN (1, 13)
                AND valor_estimado <= 500000000
                AND objeto ~* %s"""
    params: list = [LEILAO_IMOVEL_REGEX]
    if abertos:
        sql += " AND data_encerramento >= CURRENT_DATE"
    if uf:
        sql += " AND uf = %s"; params.append(uf)
    sql += " ORDER BY data_encerramento ASC NULLS LAST, valor_estimado DESC LIMIT %s"
    params.append(min(limit, 200))
    return query(sql, params)

@app.get("/radar-loteamentos", dependencies=[Depends(verify_api_key_or_admin)])
async def radar_loteamentos(uf: str = None, pop_min: int = None, pop_max: int = None, limit: int = 50):
    """Ranking de municípios pra prospecção de loteamento (tabela materializada
    pelo radar_loteamento_etl.py)."""
    # bolsa_familia: valor do mês mais recente por município (gasto social executado),
    # via LEFT JOIN LATERAL — alimenta a camada de calor "Bolsa Família" no mapa.
    sql = """SELECT r.*, bf.valor AS bolsa_familia, bf.beneficiarios AS bf_beneficiarios
             FROM radar_loteamento r
             LEFT JOIN LATERAL (
               SELECT valor, beneficiarios FROM bolsa_familia_municipio b
               WHERE b.codigo_ibge = r.municipio_ibge
               ORDER BY ano_mes DESC LIMIT 1
             ) bf ON TRUE
             WHERE r.score IS NOT NULL"""
    params: list = []
    if uf:
        sql += " AND r.uf = %s"; params.append(uf)
    if pop_min is not None:
        sql += " AND r.pop_final >= %s"; params.append(pop_min)
    if pop_max is not None:
        sql += " AND r.pop_final <= %s"; params.append(pop_max)
    # cap 1000: mapa de calor por UF precisa de todos os municípios (SP=645)
    sql += " ORDER BY r.score DESC LIMIT %s"; params.append(min(limit, 1000))
    return query(sql, params)

@app.get("/agro/municipios", dependencies=[Depends(verify_api_key_or_admin)])
async def agro_municipios(uf: str = None, limit: int = 50):
    """Perfil rural (nº de estabelecimentos agropecuários, Censo Agro/IBGE)."""
    sql = "SELECT municipio_ibge, municipio_nome, uf, estabelecimentos FROM agro_municipios WHERE estabelecimentos IS NOT NULL"
    params: list = []
    if uf:
        sql += " AND uf = %s"; params.append(uf.upper())
    sql += " ORDER BY estabelecimentos DESC LIMIT %s"; params.append(min(limit, 500))
    return query(sql, params)

@app.get("/concessoes", dependencies=[Depends(verify_api_key_or_admin)])
async def concessoes(uf: str = None, abertas: bool = True, limit: int = 60):
    """Leilões de concessão pública (saneamento, iluminação, transporte, etc.) —
    objeto contém 'concessão'. Teto de R$ 100 bi (concessão de bilhões é
    legítima; acima disso é erro de digitação da fonte)."""
    sql = """SELECT pncp_id, orgao_nome, orgao_cnpj, municipio_nome, uf, objeto,
                     valor_estimado, modalidade_nome, situacao,
                     data_publicacao, data_encerramento, url_pncp
              FROM licitacoes
              WHERE objeto ILIKE %s AND valor_estimado <= 1e11"""
    params: list = ["%concess%"]
    if abertas:
        sql += " AND data_encerramento >= CURRENT_DATE"
    if uf:
        sql += " AND uf = %s"; params.append(uf.upper())
    sql += " ORDER BY data_encerramento ASC NULLS LAST, valor_estimado DESC LIMIT %s"
    params.append(min(limit, 200))
    return query(sql, params)

@app.get("/trends", dependencies=[Depends(verify_api_key_or_admin)])
async def trends(limit: int = 20):
    """Termos em alta nas compras públicas — ordenados por variação % (radar de
    conteúdo). Materializado por trends_etl.py."""
    return query("""
        SELECT termo, freq_recente, freq_anterior, valor_recente, variacao_pct, abertas
        FROM trends_termos
        WHERE freq_recente > 0
        ORDER BY variacao_pct DESC, freq_recente DESC
        LIMIT %s
    """, (min(limit, 60),))

@app.get("/comex/municipios", dependencies=[Depends(verify_api_key_or_admin)])
async def comex_municipios(uf: str = None, fluxo: str = "export", limit: int = 30):
    fluxo = "import" if fluxo == "import" else "export"
    sql = "SELECT municipio_nome, uf, fob_usd, ano FROM comex_municipios WHERE fluxo = %s"
    params: list = [fluxo]
    if uf:
        sql += " AND uf = %s"; params.append(uf.upper())
    sql += " ORDER BY fob_usd DESC LIMIT %s"; params.append(min(limit, 200))
    return query(sql, params)

@app.get("/comex/por-uf", dependencies=[Depends(verify_api_key_or_admin)])
async def comex_por_uf(fluxo: str = "export"):
    fluxo = "import" if fluxo == "import" else "export"
    return query("""
        SELECT uf, SUM(fob_usd) AS fob FROM comex_municipios
        WHERE fluxo = %s GROUP BY uf ORDER BY fob DESC
    """, (fluxo,))

@app.get("/dashboard-live", dependencies=[Depends(verify_api_key_or_admin)])
async def dashboard_live(uf: str = None):
    """Agregado do HUD numa chamada só, opcionalmente filtrado por UF.
    Público — sem auth. Alimenta o painel full-width do site."""
    uf = (uf or "").strip().upper() or None
    fuf, puf = ("", []) if not uf else (" AND uf = %s", [uf])

    kpis = query(f"""
        SELECT COUNT(*) AS licitacoes,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor,
               COUNT(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas,
               COUNT(DISTINCT municipio_ibge) AS municipios
        FROM licitacoes WHERE 1=1{fuf}
    """, puf)[0]

    leiloes = query(f"""
        SELECT pncp_id, orgao_nome, orgao_cnpj, municipio_nome, uf, objeto,
               valor_estimado, modalidade_nome, situacao, data_publicacao, data_encerramento, url_pncp
        FROM licitacoes
        WHERE modalidade_id IN (1,13) AND valor_estimado <= 500000000
          AND objeto ~* %s AND data_encerramento >= CURRENT_DATE{fuf}
        ORDER BY data_encerramento ASC LIMIT 12
    """, [LEILAO_IMOVEL_REGEX, *puf])

    crescimento = query(f"""
        SELECT municipio_ibge, municipio_nome, uf, pop_final, crescimento_pct,
               receita_per_capita, infra_valor_12m, score
        FROM radar_loteamento
        WHERE score IS NOT NULL AND pop_final >= 5000{fuf}
        ORDER BY score DESC LIMIT 12
    """, puf)

    alertas = query(f"""
        SELECT pncp_id, orgao_nome, municipio_nome, uf, objeto, valor_estimado,
               modalidade_nome, data_publicacao, url_pncp
        FROM licitacoes
        WHERE modalidade_id IN (8,9) AND valor_estimado BETWEEN 100000 AND 500000000{fuf}
        ORDER BY valor_estimado DESC LIMIT 10
    """, puf)

    por_uf = query("""
        SELECT l.uf, COUNT(*) AS total,
               SUM(l.valor_estimado) FILTER (WHERE l.valor_estimado <= 500000000) AS valor,
               COUNT(*) FILTER (WHERE l.modalidade_id IN (1,13)
                   AND l.objeto ~* '""" + LEILAO_IMOVEL_REGEX + """'
                   AND l.data_encerramento >= CURRENT_DATE) AS leiloes_abertos,
               COALESCE(c.fob, 0) AS comex_export
        FROM licitacoes l
        LEFT JOIN (SELECT uf, SUM(fob_usd) AS fob FROM comex_municipios
                   WHERE fluxo='export' GROUP BY uf) c ON c.uf = l.uf
        WHERE l.uf IS NOT NULL AND l.uf != ''
        GROUP BY l.uf, c.fob
    """)

    termos_alta = query("""
        SELECT termo, freq_recente, variacao_pct, abertas
        FROM trends_termos WHERE freq_recente > 0
        ORDER BY variacao_pct DESC, freq_recente DESC LIMIT 15
    """)

    comex = query(f"""
        SELECT municipio_nome, uf, fob_usd FROM comex_municipios
        WHERE fluxo='export'{fuf} ORDER BY fob_usd DESC LIMIT 12
    """, puf)

    return {"uf": uf, "kpis": kpis, "leiloes": leiloes,
            "crescimento": crescimento, "alertas": alertas, "por_uf": por_uf,
            "trends": termos_alta, "comex": comex}

@app.get("/stats/gerais", dependencies=[Depends(verify_api_key_or_admin)])
async def stats_gerais():
    """Contadores agregados de todos os pilares numa chamada só — alimenta a
    seção 'prova por dados' da home do allancandido.com."""
    lic = query("""SELECT COUNT(*) AS total,
                          SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor
                   FROM licitacoes""")[0]
    imoveis = query("SELECT COUNT(*) AS total FROM imoveis_uniao")[0]
    municipios = query("SELECT COUNT(*) AS total FROM score_municipios")[0]
    empresas = query("SELECT COUNT(*) AS total FROM empresas")[0]
    return {
        "licitacoes": lic["total"],
        "licitacoes_valor": float(lic["valor"] or 0),
        "imoveis_uniao": imoveis["total"],
        "municipios": municipios["total"],
        "empresas": empresas["total"],
    }

@app.get("/municipios", dependencies=[Depends(verify_api_key_or_admin)])
async def municipios_busca(q: str = None, uf: str = None, limit: int = 20):
    """Busca de municípios (nome parcial) sobre score_municipios — usada pela
    busca pública do allancandido.com/insights."""
    sql = """SELECT municipio_ibge, municipio_nome, uf, populacao, receita_per_capita
             FROM score_municipios WHERE 1=1"""
    params = []
    if q:
        sql += " AND municipio_nome ILIKE %s"; params.append(f"%{q}%")
    if uf:
        sql += " AND uf = %s"; params.append(uf)
    sql += " ORDER BY populacao DESC NULLS LAST LIMIT %s"; params.append(min(limit, 100))
    return query(sql, params)

@app.get("/municipios/{ibge}", dependencies=[Depends(verify_api_key_or_admin)])
async def municipio_detalhe(ibge: str):
    """Visão combinada de um município (por código IBGE): saúde fiscal
    (score_municipios) + licitações agregadas — alimenta as páginas
    programáticas /insights/[municipio] do site público."""
    score = query("SELECT * FROM score_municipios WHERE municipio_ibge = %s ORDER BY exercicio DESC, periodo DESC LIMIT 1", (ibge,))
    lic = query("""
        SELECT COUNT(*) AS total,
               SUM(valor_estimado) FILTER (WHERE valor_estimado <= 500000000) AS valor,
               COUNT(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas
        FROM licitacoes WHERE municipio_ibge = %s
    """, (ibge,))
    top_lic = query("""
        SELECT pncp_id, orgao_nome, orgao_cnpj, objeto, valor_estimado,
               modalidade_nome, situacao, data_publicacao, data_encerramento, url_pncp
        FROM licitacoes
        WHERE municipio_ibge = %s AND valor_estimado <= 500000000
        ORDER BY valor_estimado DESC LIMIT 5
    """, (ibge,))
    if not score and (not lic or not lic[0]["total"]):
        raise HTTPException(status_code=404, detail="Município sem dados")
    radar = query("SELECT pop_inicial, pop_final, ano_inicial, ano_final, crescimento_pct, infra_valor_12m, score AS radar_score FROM radar_loteamento WHERE municipio_ibge = %s", (ibge,))
    agro = query("SELECT estabelecimentos FROM agro_municipios WHERE municipio_ibge = %s", (ibge,))
    return {
        "score": score[0] if score else None,
        "licitacoes": lic[0] if lic else None,
        "top_licitacoes": top_lic,
        "radar": radar[0] if radar else None,
        "agro": agro[0] if agro else None,
    }

@app.get("/score-municipios/stats", dependencies=[Depends(verify_api_key_or_admin)])
async def score_municipios_stats():
    geral = query("SELECT count(*) AS total, COALESCE(avg(receita_per_capita),0) AS media_percapita FROM score_municipios")
    por_uf = query(
        """SELECT uf, count(*) AS total, COALESCE(avg(receita_per_capita),0) AS media_percapita
           FROM score_municipios GROUP BY uf ORDER BY media_percapita DESC"""
    )
    return {"total": geral[0]["total"], "media_percapita": float(geral[0]["media_percapita"]), "por_uf": por_uf}

# ─── PDF ──────────────────────────────────────────────────────────────────────
@app.get("/pdf", dependencies=[Depends(verify_admin)])
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
