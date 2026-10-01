"""
Área de imóvel em m² a partir do texto do anúncio — inclusive rural.

Anúncio de sítio/fazenda costuma trazer a área em hectare ou alqueire, não em
m² ("Fazenda 12 alqueires", "Sítio de 3,5 ha"). Os coletores gravam tudo em
m² na coluna `area`; quem mostra converte de volta para R$/ha e R$/alqueire.

Alqueire varia por região: o goiano/mineiro/geométrico (GO, MG, RJ, ES, BA…)
tem 48.400 m² (4,84 ha); o paulista (SP, e é o usual no PR) tem 24.200 m².
"""
import re

M2_POR_HA = 10_000
M2_ALQUEIRE_PAULISTA = 24_200
M2_ALQUEIRE_GOIANO = 48_400
UFS_ALQUEIRE_PAULISTA = {"SP", "PR"}

# Tipos que são imóvel rural por natureza (o resto vira rural pela área).
TIPOS_RURAIS = ("sítio", "chácara", "fazenda")
# A partir de 1 ha, terreno/área entra no recorte rural mesmo sem o tipo.
AREA_MIN_RURAL_M2 = M2_POR_HA


def m2_por_alqueire(uf):
    return M2_ALQUEIRE_PAULISTA if (uf or "").upper() in UFS_ALQUEIRE_PAULISTA else M2_ALQUEIRE_GOIANO


def _numero_br(txt):
    """'48.400' -> 48400, '3,5' -> 3.5, '1.234,5' -> 1234.5."""
    txt = txt.strip()
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", txt):
        txt = txt.replace(".", "")
    try:
        return float(txt)
    except ValueError:
        return None


_NUM = r"(\d[\d.]*(?:,\d+)?)"
_PADROES = [
    (re.compile(_NUM + r"\s*(?:hectares?|ha)\b", re.I), "ha"),
    (re.compile(_NUM + r"\s*(?:alqueires?|alq\.?)(?=\W|$)", re.I), "alq"),
    (re.compile(_NUM + r"\s*(?:m²|m2|metros?\s+quadrados?)", re.I), "m2"),
]


def area_m2_do_texto(texto, uf=None):
    """Primeira área encontrada no texto, convertida para m². None se não achar.
    Hectare e alqueire têm prioridade sobre m² (em anúncio rural o m² que
    aparece costuma ser o da casa sede, não o da terra)."""
    if not texto:
        return None
    for padrao, unidade in _PADROES:
        m = padrao.search(texto)
        if not m:
            continue
        n = _numero_br(m.group(1))
        if not n:
            continue
        if unidade == "ha":
            return round(n * M2_POR_HA, 2)
        if unidade == "alq":
            return round(n * m2_por_alqueire(uf), 2)
        return round(n, 2)
    return None


def eh_rural(tipo, area_m2):
    return (tipo or "") in TIPOS_RURAIS or (area_m2 or 0) >= AREA_MIN_RURAL_M2
