# -*- coding: utf-8 -*-
# AgloMetrics — versión web con Streamlit (sin doble conteo de ácido)

# ======================= IMPORTS =======================
import os, json, calendar, unicodedata, warnings
from datetime import datetime, date
from typing import Optional, Tuple, Any, List
from pathlib import Path

import numpy as np
import pandas as pd
import altair as alt
import joblib
import sklearn
import streamlit as st

# ---------------------------- CONFIG STREAMLIT (DEBE SER EL PRIMER COMANDO DE STREAMLIT) ----------------------------
st.set_page_config(page_title="AgloMetrics", layout="wide")

# ======================= CONSTANTES =======================
class Proc:
    OBJ_P80   = 80.0
    ISG_SET   = 50.0
    GAMMA     = 0.50
    RHO_W     = 1.0        # t/m3
    RHO_H2SO4 = 1.84       # t/m3
    MRATIO    = 98.0/63.55 # kg H2SO4 / kg Cu
    TRES_7RPM = 0.80       # min a 7 rpm (ajustable)

HIST_HEAD = [
    "Fecha","Ingeniero","Turno","Ciclo","Modulo",
    "TPH","Hum%","Agua_kg_t","Agua_m3_h","Acido_kg_t",
    "CuT%","CuS%","CO3%","NO3%","CAN_mina_kg_t",
    "Origen","RAL","P80_real%","P25%","Finos_100#_%",
    "Ton_turno_t","RPM",
    "Acido_m3_h_calc","P80_est%","ISG_est%",
    "Perd_kgCu_h","Perd_turno_kg","Perd_dia_kg"
]
MOD_HEAD = ["FechaHora","Ciclo","Modulo","Ton_total_t","Acido_m3","Agua_m3",
            "P80_real%","Finos_100#_%","Perdida_Cu_texto","ISG_est_mod%","ISG_real%","Observaciones"]
SULF_HEAD = ["FechaHora","Ciclo","Modulo","ISG_est%","ISG_real%","Fuente","Observaciones"]

MONTHS = {"enero":1,"ene":1,"febrero":2,"feb":2,"marzo":3,"mar":3,"abril":4,"abr":4,
          "mayo":5,"may":5,"junio":6,"jun":6,"julio":7,"jul":7,"agosto":8,"ago":8,
          "septiembre":9,"sep":9,"octubre":10,"oct":10,"noviembre":11,"nov":11,
          "diciembre":12,"dic":12}

# ======================= UTILIDADES =======================
def export_dir() -> str:
    # En Streamlit Cloud, el cwd es escribible durante la sesión
    return os.path.abspath(os.getcwd())

def clamp(x,a,b): return max(a,min(b,x))
def nonneg(x): return max(0.0,x)

def to_float(s):
    try: return float(str(s).replace(",", ".").strip())
    except: return 0.0

def pretty_kg(v):
    try: return f"{float(v):,.0f}".replace(",", ".")
    except: return str(v)

def _ts(): return datetime.now().strftime("%Y%m%d_%H%M%S")

def export_rows_to_excel(base_name: str, headers: List[str], rows: List[List[Any]]):
    out_xlsx = os.path.join(export_dir(), f"{base_name}_{_ts()}.xlsx")
    try:
        df = pd.DataFrame(rows, columns=headers)
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
            df.to_excel(xw, index=False, sheet_name="datos")
        return out_xlsx
    except Exception as e:
        return f"Error exportando: {e}"

def open_path_hint(path: str):
    st.success(f"Archivo generado: **{os.path.basename(path)}**\n\nRuta: `{path}`")

def parse_date_any(s: str)->Optional[Tuple[int,int,Optional[int]]]:
    if not s: return None
    t=str(s).lower().strip().replace('.', ' ').replace(',', ' ')
    import re
    t=re.sub(r'\bde\b',' ',t); t=re.sub(r'\s+',' ',t)
    for n,m in MONTHS.items(): t=re.sub(rf'\b{n}\b',f'{m}',t)
    t=t.replace('-', ' ').replace('/', ' ')
    nums=[p for p in t.split() if re.fullmatch(r'\d+',p)]
    if len(nums)>=2:
        d,m=int(nums[0]),int(nums[1]); y=int(nums[2]) if len(nums)>=3 else None
        if y is not None and y<100: y = y+2000 if y<=69 else y+1900
        return (d,m,y)
    return None

def csv_datetime(s: str):
    try:
        s=str(s).strip()
        if len(s)>10: return datetime.strptime(s,"%d/%m/%Y %H:%M")
        return datetime.strptime(s,"%d/%m/%Y")
    except: return None

def strip_accents(txt: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', txt) if unicodedata.category(c) != 'Mn')

def normalize_hist_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=HIST_HEAD)
    mapping = {
        "fecha":"Fecha","ingeniero":"Ingeniero","turno":"Turno","ciclo":"Ciclo",
        "modulo":"Modulo","módulo":"Modulo","módulo ":"Modulo",
        "tph":"TPH","hum%":"Hum%","humedad":"Hum%","humedad %":"Hum%",
        "agua kg/t":"Agua_kg_t","agua kg_t":"Agua_kg_t",
        "agua m3/h":"Agua_m3_h","agua m3_h":"Agua_m3_h",
        "acido kg/t":"Acido_kg_t","ácido kg/t":"Acido_kg_t","acido kg_t":"Acido_kg_t",
        "cut%":"CuT%","cus%":"CuS%","co3%":"CO3%","no3%":"NO3%","no3 (g/l)":"NO3%","no3":"NO3%",
        "can mina kg/t":"CAN_mina_kg_t",
        "origen":"Origen","ral":"RAL",
        "p80 real%":"P80_real%","p25%":"P25%",
        "finos -100# %":"Finos_100#_%","finos 100# %":"Finos_100#_%","finos #100 (%)":"Finos_100#_%",
        "ton turno t":"Ton_turno_t","rpm":"RPM",
        "acido m3/h calc":"Acido_m3_h_calc",
        "p80 est%":"P80_est%","isg est%":"ISG_est%",
        "perd kgcu/h":"Perd_kgCu_h","perd turno kg":"Perd_turno_kg","perd dia kg":"Perd_dia_kg"
    }
    new = {}
    for c in df.columns:
        key = strip_accents(str(c)).lower().strip()
        tgt = mapping.get(key)
        if tgt: new[c] = tgt
    df = df.rename(columns=new)
    for c in HIST_HEAD:
        if c not in df.columns: df[c] = np.nan
    return df[HIST_HEAD]

def normalize_mod_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=MOD_HEAD)
    mapping = {
        "fechahora":"FechaHora","ciclo":"Ciclo","modulo":"Modulo","módulo":"Modulo",
        "ton_total_t":"Ton_total_t","acido_m3":"Acido_m3","agua_m3":"Agua_m3",
        "p80_real%":"P80_real%","finos_100#_%":"Finos_100#_%","finos -100# %":"Finos_100#_%",
        "perdida_cu_texto":"Perdida_Cu_texto","isg_est_mod%":"ISG_est_mod%","isg_real%":"ISG_real%","observaciones":"Observaciones"
    }
    new={}
    for c in df.columns:
        key = strip_accents(str(c)).lower().strip()
        tgt = mapping.get(key)
        if tgt: new[c] = tgt
    df = df.rename(columns=new)
    for c in MOD_HEAD:
        if c not in df.columns: df[c] = np.nan
    return df[MOD_HEAD]

def normalize_sulf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=SULF_HEAD)
    mapping = {"fechahora":"FechaHora","ciclo":"Ciclo","modulo":"Modulo",
               "isg_est%":"ISG_est%","isg_real%":"ISG_real%","fuente":"Fuente","observaciones":"Observaciones"}
    new={}
    for c in df.columns:
        key = strip_accents(str(c)).lower().strip()
        tgt = mapping.get(key)
        if tgt: new[c] = tgt
    df = df.rename(columns=new)
    for c in SULF_HEAD:
        if c not in df.columns: df[c] = np.nan
    return df[SULF_HEAD]

# ======================= ARCHIVOS CSV =======================
def csv_paths():
    base = export_dir()
    return (os.path.join(base,"humedades.csv"),
            os.path.join(base,"modulos_termino.csv"),
            os.path.join(base,"sulfatacion.csv"),
            os.path.join(base,"ultimo_form.json"))

def ensure_headers(path: str, head: List[str]):
    if not os.path.exists(path):
        pd.DataFrame(columns=head).to_csv(path, index=False, encoding="utf-8", sep=',')

def read_csv_df(path: str, headers_expected: List[str]|None=None, kind: str="") -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=headers_expected or [])
    try:
        df = pd.read_csv(path, encoding="utf-8", sep=',', header=0, engine="python")
    except Exception:
        return pd.DataFrame(columns=headers_expected or [])
    if kind=="hist":
        df = normalize_hist_columns(df)
    elif kind=="mod":
        df = normalize_mod_columns(df)
    elif kind=="sulf":
        df = normalize_sulf_columns(df)
    if headers_expected:
        for col in headers_expected:
            if col not in df.columns:
                df[col] = np.nan
        df = df[headers_expected]
    return df

def write_csv_df(path: str, df: pd.DataFrame):
    df.to_csv(path, index=False, encoding="utf-8", sep=',')

# ======================= MODELOS =======================
try:
    import skops.io as skio
    _HAS_SKOPS = True
except Exception:
    _HAS_SKOPS = False

def _safe_version_warning(meta: dict):
    try:
        trained = meta.get("sklearn_trained")
        ver_run = sklearn.__version__
        if trained and trained != ver_run:
            warnings.warn(
                f"Modelo entrenado con scikit-learn {trained} y ejecutando en {ver_run}. "
                f"Si notas diferencias, re-exporta a .skops o alinea versiones.",
                category=UserWarning
            )
    except Exception:
        pass

def _unpack_model_object(obj):
    if isinstance(obj, dict):
        model = obj.get("model", obj)
        feats = obj.get("features") or []
        meta = {
            "target": obj.get("target"),
            "metrics": obj.get("metrics") or {},
            "sklearn_trained": obj.get("sklearn"),
            "trained_at": obj.get("trained_at"),
        }
    else:
        model, feats, meta = obj, [], {"metrics": {}}
    _safe_version_warning(meta)
    return model, feats, meta

def load_model_safely(path: str | Path):
    base = Path(path)
    candidates = [base] if base.suffix else [base.with_suffix(".skops"), base.with_suffix(".pkl")]
    last_err = None
    for p in candidates:
        if not p.exists():
            last_err = FileNotFoundError(f"No existe: {p}")
            continue
        try:
            if p.suffix == ".skops":
                if not _HAS_SKOPS:
                    raise ImportError("skops no instalado (agrega 'skops' a requirements.txt).")
                obj = skio.load(p, trusted=True)
            else:
                obj = joblib.load(p)
            return _unpack_model_object(obj)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No se pudo cargar el modelo desde {base} (.skops/.pkl). Último error: {last_err}")

@st.cache_resource(show_spinner=False)
def load_all_models():
    """Busca p80_model e isg_rf en /models y en la raíz; prioriza .skops sobre .pkl."""
    base = Path(__file__).parent
    models_dir = base / "models"

    def find_model(base_name: str) -> Path | None:
        checks = [
            models_dir / f"{base_name}.skops",
            models_dir / f"{base_name}.pkl",
            base / f"{base_name}.skops",
            base / f"{base_name}.pkl",
        ]
        for p in checks:
            if p.exists():
                return p
        return None

    modelos, errores = {}, {}
    for name in ("p80_model", "isg_rf"):
        p = find_model(name)
        if not p:
            errores[name] = f"No se encontró {name}.skops/.pkl en ./models ni en la raíz."
            continue
        try:
            modelos[name] = load_model_safely(p)
        except Exception as e:
            errores[name] = str(e)
    return modelos, errores

MODELOS, MODELOS_ERR = load_all_models()

# ======================= CÁLCULOS =======================
def residence_time_min(rpm: float) -> float:
    rpm=max(0.1,rpm)
    return Proc.TRES_7RPM * (7.0 / rpm)

def humedad_balance(h0, tph, agua_m3h=0.0, acido_m3h=0.0, agua_kgt=0.0, acido_kgt=0.0):
    """
    Humedad por balance. Evita doble conteo:
    - Considera agua_m3h (como caudal total por flautas de agua, incluya o no RAL).
    - Considera acido_kgt (ácido por flautas de ácido).
    - Ignora acido_m3h (dejar en 0.0 en llamadas).
    """
    if tph<=0: return clamp(h0,0.0,100.0)
    agua_k = nonneg(agua_kgt) + (nonneg(agua_m3h)*Proc.RHO_W*1000)/max(tph,1e-9)
    # acido_k: solo desde kg/t (NO sumar m3/h de ácido para no duplicar)
    acido_k = nonneg(acido_kgt)
    return clamp(h0 + 0.1*(agua_k+acido_k), 0.0, 100.0)

def evaluar_p80(p80,cu,tph):
    deficit = nonneg(Proc.OBJ_P80 - p80)
    frac = Proc.GAMMA * (deficit/100.0)
    kg_cu_h = tph*1000*(cu/100.0)*frac
    estado = f"P80: {p80:.1f}% OK" if deficit<=0 else f"P80: {p80:.1f}% (deficit {deficit:.1f} pts)"
    return estado, kg_cu_h

def calc_isg_formula(cu, cu_sol, tph, agua_m3h, acid_gpl, acido_kgt, extra_kgt=0.0):
    """
    Ácido disponible:
      - Desde flautas de ácido: tph * acido_kgt  (kg/h)
      - Desde RAL: si acid_gpl>0, agua_m3h * acid_gpl  (kg/h, g/L ≈ kg/m3)
    SIN fallback de tratar caudal como ácido puro.
    """
    cu_use   = cu_sol if cu_sol>0 else cu
    cu_kg_h  = tph*1000*(cu_use/100.0)
    need     = Proc.MRATIO*cu_kg_h + max(0.0, extra_kgt)*max(tph,0.0)

    avail_kgph = 0.0
    if acido_kgt>0:
        avail_kgph += tph*acido_kgt
    if acid_gpl>0 and agua_m3h>0:
        avail_kgph += agua_m3h*acid_gpl

    grado = 0.0 if need<=0 else avail_kgph/max(need,1e-9)
    return clamp(grado*100.0, 0.0, 100.0), need, avail_kgph, (avail_kgph-need)

def finos_flag(f):
    if f is None: return "—"
    return "Rojo" if f>30 else ("Ambar" if f>=25 else "OK")

# ======================= SIDEBAR =======================
if os.path.exists("AgloMetrics_P80_icon_512.png"):
    st.sidebar.image("AgloMetrics_P80_icon_512.png", width=240)

st.sidebar.title("AgloMetrics — Web")
st.sidebar.subheader("Optimizacion aglomerado con ML")
solo_lectura = st.sidebar.toggle("🔒 Modo Solo Lectura", value=False,
                                 help="Bloquea escritura en CSV y ediciones.")

# Estado de modelos
model_isg  = None; isg_feats=None; isg_meta={}
model_p80  = None; p80_feats=None; p80_meta={}
if "isg_rf" in MODELOS:
    model_isg, isg_feats, isg_meta = MODELOS["isg_rf"]
if "p80_model" in MODELOS:
    model_p80, p80_feats, p80_meta = MODELOS["p80_model"]

col_m1, col_m2 = st.sidebar.columns(2)
with col_m1:
    st.success("Modelo ISG cargado") if model_isg else st.warning("ISG no cargado")
with col_m2:
    st.success("Modelo P80 cargado") if model_p80 else st.warning("P80 no cargado")

if MODELOS_ERR:
    st.sidebar.markdown("**Detalles de modelos no cargados:**")
    for k,v in MODELOS_ERR.items():
        st.sidebar.markdown(f"- **{k}**: {v}")

# ======================= RUTAS CSV =======================
csv_hist, csv_mod, csv_sulf, last_form = csv_paths()

# ======================= TABS =======================
tabs = st.tabs([
    "Ingreso", "Historicos", "Termino Modulo",
    "Hist. Modulos", "Hist. Sulfatacion", "Simulador / Optimizacion"
])

# ------------------------------------------------ Ingreso
with tabs[0]:
    st.subheader("Ingreso — Control de Aglomeracion")

    defaults = {}
    if st.session_state.get("load_defaults") and os.path.exists(last_form):
        try:
            defaults = json.load(open(last_form,"r",encoding="utf-8"))
        except:
            defaults = {}
        st.session_state.pop("load_defaults", None)

    c1,c2,c3,c4 = st.columns([1,0.7,0.6,0.8])
    fecha_ui  = c1.text_input("Fecha del registro (dd/mm/aaaa)", value=date.today().strftime("%d/%m/%Y"), key="ing_fecha")
    ingeniero = c2.text_input("Ingeniero", value=defaults.get("ingeniero",""), key="ing_ingeniero")
    turno     = c3.selectbox("Turno", ["A","B"], index=0 if defaults.get("turno","A").upper()!="B" else 1, key="ing_turno")
    ciclo     = c4.text_input("Ciclo modulo", value=defaults.get("ciclo",""), key="ing_ciclo")

    c1,c2,c3,c4 = st.columns(4)
    modulo   = c1.text_input("Numero modulo", value=defaults.get("modulo",""), key="ing_modulo")
    tph      = c2.number_input("TPH pasante (t/h)", min_value=0.0, value=to_float(defaults.get("tph",0)), key="ing_tph")
    hum      = c3.number_input("Humedad inicial (%)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("hum",0)), key="ing_h")
    agua_kgt = c4.number_input("Agua en flautas (kg/t)", min_value=0.0, value=to_float(defaults.get("agua_kgt",0)), key="ing_agua_kgt")

    c1,c2,c3,c4 = st.columns(4)
    agua_m3h = c1.number_input("Agua m3/h", min_value=0.0, value=to_float(defaults.get("agua_m3h",0)), key="ing_agua_m3h")
    acid_kgt = c2.number_input("Acido (kg/t)", min_value=0.0, value=to_float(defaults.get("acid_kgt",0)), key="ing_acid_kgt")
    cut      = c3.number_input("CuT (%)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("cut",0)), key="ing_cut")
    cus      = c4.number_input("Cu soluble (%)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("cus",0)), key="ing_cus")

    c1,c2,c3,c4 = st.columns(4)
    carb     = c1.number_input("CO3 (%)", min_value=0.0, value=to_float(defaults.get("carb",0)), key="ing_carb")
    nitr     = c2.number_input("NO3 (g/L)", min_value=0.0, value=to_float(defaults.get("nitr",0)), key="ing_nitr")
    can_mina = c3.number_input("CAN mina (kg/t)", min_value=0.0, value=to_float(defaults.get("can_mina",0)), key="ing_can")
    origen   = c4.text_input("Origen de alimentacion", value=defaults.get("origen",""), key="ing_origen")

    c1,c2,c3,c4 = st.columns(4)
    acid_gpl = c1.number_input("RAL (g/L)", min_value=0.0, value=to_float(defaults.get("acid_gpl",0)), key="ing_ral")
    p80      = c2.number_input("P80 (% pasante a 12.7 mm)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("p80",Proc.OBJ_P80)), key="ing_p80")
    p25      = c3.number_input("1/4'' (% pasante a 6.3 mm)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("p25",0)), key="ing_p25")
    finos    = c4.number_input("Finos #100 (% pasante)", min_value=0.0, max_value=100.0, value=to_float(defaults.get("finos",0)), key="ing_finos")

    c1,c2 = st.columns(2)
    turno_t = c1.number_input("Tonelaje total del turno (t)", min_value=0.0, value=to_float(defaults.get("turno_t",0)), key="ing_turnot")
    rpm     = c2.number_input("RPM tambores", min_value=0.1, value=max(0.1, to_float(defaults.get("rpm",7))), key="ing_rpm")

    c1,c2,c3,c4 = st.columns([1,1,1,1])
    calcular = c1.button("CALCULAR", key="ing_calc")
    guardar  = c2.button("GUARDAR", disabled=solo_lectura, key="ing_save")
    exportar = c3.button("EXPORTAR Excel", key="ing_export")
    limpiar  = c4.button("LIMPIAR", key="ing_clear")

    if limpiar:
        keys = [k for k in st.session_state.keys() if k.startswith("ing_")]
        for k in keys: st.session_state.pop(k, None)
        st.rerun()

    # ---------- CÁLCULOS ----------
    if calcular or guardar:
        errs = []
        if tph<=0: errs.append("TPH debe ser > 0 para calculos completos.")
        if cus>cut: errs.append("Cu soluble no puede exceder CuT.")
        for msg in errs: st.warning("⚠️ "+msg)

        # Humedad: agua_m3h + ácido kg/t (sin sumar flujo de ácido en m3/h para evitar doble conteo)
        h_bal = humedad_balance(hum, tph, agua_m3h, 0.0, agua_kgt, acid_kgt)
        st.info(f"**Humedad por balance:** {h_bal:.2f}% — Objetivo 10–12% → " + ("OK" if 10.0<=h_bal<=12.0 else "Fuera de rango"))

        estado_p80, kg_cu_h = evaluar_p80(p80, cut, tph)
        st.write(f"**{estado_p80}** | 1/4'' muestrera: {p25:.1f}% pasante")
        st.write(f"**Perdida estimada:** {pretty_kg(kg_cu_h)} kg Cu/h")
        perd_turno = kg_cu_h*(turno_t/max(tph,1e-9)) if (turno_t>0 and tph>0) else 0.0
        perd_dia = kg_cu_h*24.0
        st.write(f"Perdida turno: {pretty_kg(perd_turno)} kg Cu | Perdida dia: {pretty_kg(perd_dia)} kg Cu")
        st.write(f"Finos #100: {finos:.1f}% → **{finos_flag(finos)}**")

        delta = acid_kgt - can_mina
        pct = (delta/can_mina*100.0) if can_mina>0 else 0.0
        st.write(f"Acido flauta: {acid_kgt:.2f} | CAN mina: {can_mina:.2f} | Δ {delta:+.2f} kg/t ({pct:+.1f}%)")

        # ISG (modelo o fórmula): ácido disponible = flautas (kg/t) + RAL (agua_m3h * g/L)
        agua_kgt_full = agua_kgt + (agua_m3h*Proc.RHO_W*1000.0/max(tph,1e-9) if tph>0 else 0.0)
        t_res = residence_time_min(rpm)

        isg_formula, need_kgph, avail_kgph, diff_kgph = calc_isg_formula(
            cut, cus, tph, agua_m3h, acid_gpl, acid_kgt
        )
        diff_kgt = diff_kgph / max(tph, 1e-9) if tph>0 else 0.0

        feature_map = {
            "humedad_balance": h_bal, "hum%": h_bal,
            "cut%": cut, "cut": cut,
            "cus%": cus, "cus": cus,
            "no3": nitr, "no3_gpl": nitr,
            "co3": carb, "co3_%": carb,
            "tph": tph,
            "acid_kgt": acid_kgt, "acido_kgt": acid_kgt,
            "agua_kgt_total": agua_kgt_full,
            "rpm": rpm,
            "t_res_min": t_res, "tres_min": t_res,
            "ral_gpl": acid_gpl,
            "finos_%": finos, "f100": finos
        }
        if model_isg is not None:
            try:
                vec = [feature_map.get((f if isinstance(f,str) else str(f)).lower(), 0.0) for f in (isg_feats or [])]
                if not vec: vec = [h_bal, cut, cus, nitr, carb, tph, acid_kgt, agua_kgt_full, rpm, t_res, acid_gpl, finos]
                isg_est = float(model_isg.predict([vec])[0])
                isg_est = clamp(isg_est,0,100)
                fuente = "Modelo ML"
            except Exception:
                isg_est = isg_formula; fuente = "Formula (fallback)"
        else:
            isg_est = isg_formula; fuente = "Formula"

        st.success(f"Sulfatacion estimada (ISG): **{isg_est:.1f}%** · {fuente}")

        if isg_est >= Proc.ISG_SET:
            st.success(f"✅ ISG cumple objetivo ({Proc.ISG_SET:.0f}%). Acido disp. **{pretty_kg(avail_kgph)}** kg/h, req. **{pretty_kg(need_kgph)}** kg/h (Δ {pretty_kg(diff_kgph)} kg/h, {diff_kgt:+.2f} kg/t).")
        else:
            st.warning(f"⚠️ ISG bajo objetivo ({Proc.ISG_SET:.0f}%). Faltan ≈ **{pretty_kg(-diff_kgph)} kg/h** de H2SO4 (≈ {abs(diff_kgt):.2f} kg/t).")

        # ---------- Guardar ----------
        if guardar and not solo_lectura:
            ensure_headers(csv_hist, HIST_HEAD)
            p80_est = Proc.OBJ_P80
            if model_p80 is not None:
                try:
                    fm = {"humedad_balance":h_bal,"hum%":h_bal,"cut%":cut,"cut":cut,"tph":tph,
                          "f100":finos, "finos":finos, "co3":carb,"no3":nitr}
                    vec_p80 = [fm.get((f if isinstance(f,str) else str(f)).lower(), 0.0) for f in (p80_feats or [])]
                    if not vec_p80: vec_p80 = [h_bal, cut, tph, finos, carb, nitr]
                    p80_est = float(model_p80.predict([vec_p80])[0])
                except Exception:
                    p80_est = Proc.OBJ_P80

            # Solo referencia de conversión: m3/h equivalentes desde kg/t (no usado en cálculos)
            acido_m3_h_calc = (tph*acid_kgt)/(Proc.RHO_H2SO4*1000.0) if (acid_kgt>0 and tph>0) else 0.0

            row = {
                "Fecha": fecha_ui,
                "Ingeniero": ingeniero, "Turno": turno, "Ciclo": ciclo, "Modulo": modulo,
                "TPH": f"{tph:g}", "Hum%": f"{hum:g}", "Agua_kg_t": f"{agua_kgt:g}", "Agua_m3_h": f"{agua_m3h:g}", "Acido_kg_t": f"{acid_kgt:g}",
                "CuT%": f"{cut:g}", "CuS%": f"{cus:g}", "CO3%": f"{carb:g}", "NO3%": f"{nitr:g}", "CAN_mina_kg_t": f"{can_mina:g}",
                "Origen": origen, "RAL": f"{acid_gpl:g}",
                "P80_real%": f"{p80:.1f}", "P25%": f"{p25:.1f}", "Finos_100#_%": f"{finos:.1f}",
                "Ton_turno_t": f"{turno_t:g}", "RPM": f"{rpm:g}",
                "Acido_m3_h_calc": f"{acido_m3_h_calc:.3f}",
                "P80_est%": f"{p80_est:.1f}", "ISG_est%": f"{isg_est:.1f}",
                "Perd_kgCu_h": f"{kg_cu_h:.0f}", "Perd_turno_kg": f"{perd_turno:.0f}", "Perd_dia_kg": f"{perd_dia:.0f}",
            }
            df = read_csv_df(csv_hist, HIST_HEAD, kind="hist")
            for col in HIST_HEAD:
                if col not in df.columns: df[col] = np.nan
            df = df[HIST_HEAD]
            df.loc[len(df)] = [row[c] for c in HIST_HEAD]
            write_csv_df(csv_hist, df)

            # Sulfatación estimada (sincroniza por ciclo si no hay real)
            ensure_headers(csv_sulf, SULF_HEAD)
            dfs = read_csv_df(csv_sulf, SULF_HEAD, kind="sulf")
            idx_to_update = None
            if not dfs.empty:
                same_ciclo = dfs["Ciclo"].astype(str).str.strip() == str(ciclo).strip()
                no_real = dfs["ISG_real%"].astype(str).str.strip().eq("").fillna(True)
                cand = dfs[same_ciclo & no_real]
                if not cand.empty:
                    idx_to_update = cand.index[-1]
            fecha_hora = f"{fecha_ui} {datetime.now().strftime('%H:%M')}"
            if idx_to_update is not None:
                dfs.loc[idx_to_update, "FechaHora"] = fecha_hora
                dfs.loc[idx_to_update, "Modulo"]    = str(modulo)
                dfs.loc[idx_to_update, "ISG_est%"]  = f"{isg_est:.1f}"
                dfs.loc[idx_to_update, "Fuente"]    = "Modelo ML" if model_isg else "Formula"
            else:
                dfs.loc[len(dfs)] = [fecha_hora, ciclo, modulo, f"{isg_est:.1f}", "", ("Modelo ML" if model_isg else "Formula"), ""]
            write_csv_df(csv_sulf, dfs)

            # último form (comodidad de UI)
            data = dict(ingeniero=ingeniero, turno=turno, ciclo=ciclo, modulo=modulo,
                        tph=str(tph), hum=str(hum), agua_kgt=str(agua_kgt), agua_m3h=str(agua_m3h),
                        acid_kgt=str(acid_kgt), cut=str(cut), cus=str(cus), carb=str(carb), nitr=str(nitr),
                        can_mina=str(can_mina), origen=origen, acid_gpl=str(acid_gpl),
                        p80=str(p80), p25=str(p25), finos=str(finos), turno_t=str(turno_t), rpm=str(rpm))
            try:
                json.dump(data, open(last_form,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
            except:
                pass

            st.success("Registro guardado (histórico + sulfatación estimada).")

    if exportar:
        df = read_csv_df(csv_hist, HIST_HEAD, kind="hist")
        if df.empty:
            st.warning("No hay registros para exportar.")
        else:
            path = export_rows_to_excel("historicos_humedades", list(df.columns), df.values.tolist())
            open_path_hint(path)

# ----------------------------------------------- Historicos
with tabs[1]:
    st.subheader("Historicos — Humedades / ISG / Perdidas")
    df = read_csv_df(csv_hist, HIST_HEAD, kind="hist")
    if df.empty:
        st.info("Sin registros todavia.")
    else:
        f1,f2,f3,f4,f5,f6 = st.columns([1,1,1,1,1,1])
        texto = f1.text_input("Texto libre", key="hist_texto")
        fecha = f2.text_input("Fecha puntual (cualquier formato)", key="hist_fecha_puntual")
        desde = f3.text_input("Desde (dd/mm/aaaa)", key="hist_desde")
        hasta = f4.text_input("Hasta (dd/mm/aaaa)", key="hist_hasta")
        fciclo = f5.text_input("Ciclo", key="hist_ciclo")
        fmod   = f6.text_input("Modulo", key="hist_modulo")

        dfv = df.copy()
        if texto:
            mask = dfv.apply(lambda r: texto.lower() in (" ".join(map(str, r.values))).lower(), axis=1)
            dfv = dfv[mask]
        if "Fecha" in dfv.columns:
            if fecha:
                tgt = parse_date_any(fecha)
                if tgt:
                    td,tm,ty = tgt
                    def _ok(x):
                        d = csv_datetime(str(x))
                        if not d: return False
                        return (d.day==td and d.month==tm and (ty is None or d.year==ty))
                    dfv = dfv[dfv["Fecha"].apply(_ok)]
            if desde:
                ddes = csv_datetime(desde)
                if ddes is not None:
                    dfv = dfv[dfv["Fecha"].apply(lambda x: (csv_datetime(str(x)) or datetime(1900,1,1))>=ddes)]
            if hasta:
                dhas = csv_datetime(hasta)
                if dhas is not None:
                    dfv = dfv[dfv["Fecha"].apply(lambda x: (csv_datetime(str(x)) or datetime(9999,1,1))<=dhas)]
        if fciclo: dfv = dfv[dfv["Ciclo"].astype(str).str.contains(fciclo, case=False, na=False)]
        if fmod:   dfv = dfv[dfv["Modulo"].astype(str).str.contains(fmod, case=False, na=False)]

        st.caption(f"{len(dfv)} registro(s)")

        if not isinstance(dfv, pd.DataFrame):
            st.error("Vista de datos no válida.")
        else:
            edited = st.data_editor(dfv, num_rows="dynamic" if not solo_lectura else "fixed", disabled=solo_lectura, key="hist_editor")

            c1,c2,c3 = st.columns([1,1,1])
            exp_pdf = c1.button("Exportar PDF", key="hist_exp_pdf")
            exp_xls = c2.button("Exportar Excel", key="hist_exp_xls")
            aplicar = c3.button("Aplicar cambios al CSV", disabled=solo_lectura, key="hist_apply")

            if exp_xls:
                path = export_rows_to_excel("historicos_humedades_vista", list(edited.columns), edited.values.tolist())
                open_path_hint(path)

            if exp_pdf:
                try:
                    from reportlab.lib.pagesizes import landscape, A4
                    from reportlab.pdfgen import canvas as pdf
                    out=os.path.join(export_dir(), f"historicos_{_ts()}.pdf")
                    c=pdf.Canvas(out,pagesize=landscape(A4)); w,h=landscape(A4); y=h-40
                    c.setFont("Helvetica-Bold",16); c.drawString(40,y,"AgloMetrics - Historial")
                    y-=24
                    c.setFont("Helvetica",10); c.drawString(40,y,f"Filas: {len(edited)}"); y-=16
                    headers=list(edited.columns)
                    c.setFont("Helvetica-Bold",8); c.drawString(40,y," | ".join(map(str,headers))); y-=12
                    c.setFont("Helvetica",7)
                    for _,r in edited.iterrows():
                        c.drawString(40,y," | ".join(map(lambda x: str(x), r.values))); y-=11
                        if y<60:
                            c.showPage(); y=h-40
                            c.setFont("Helvetica-Bold",8); c.drawString(40,y," | ".join(map(str,headers))); y-=12
                            c.setFont("Helvetica",7)
                    c.showPage(); c.save()
                    open_path_hint(out)
                except Exception as e:
                    st.error(f"Error PDF (opcional): {e}. Instala reportlab si lo necesitas.")

            if aplicar and not solo_lectura:
                base = df.copy()
                base.loc[edited.index, :] = edited
                write_csv_df(csv_hist, base)
                st.success("Cambios aplicados.")

        # --------- GRAFICOS MENSUALES ----------
        st.markdown("### Series temporales (vista mensual)")
        gdf = df.copy()
        gdf["__dt"] = gdf["Fecha"].apply(lambda x: csv_datetime(str(x)))
        gdf = gdf.dropna(subset=["__dt"])
        if gdf.empty:
            st.info("No hay fechas validas para graficar.")
        else:
            for c in ["ISG_est%","P80_real%"]:
                if c in gdf.columns: gdf[c] = pd.to_numeric(gdf[c], errors="coerce")
            gdf["Año"] = gdf["__dt"].dt.year
            gdf["Mes"] = gdf["__dt"].dt.month

            yms = sorted(gdf[["Año","Mes"]].drop_duplicates().values.tolist())
            _N = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
            sel_idx = len(yms)-1 if yms else 0
            sel = st.selectbox("Mes", options=list(range(len(yms))),
                               index=sel_idx,
                               format_func=lambda i: f"{_N[yms[i][1]]} {yms[i][0]}" if yms else "")
            if yms:
                y_sel, m_sel = yms[sel]
                gsel = gdf[(gdf["Año"]==y_sel) & (gdf["Mes"]==m_sel)].copy()
                gsel["Dia"] = gsel["__dt"].dt.day

                if "ISG_est%" in gsel.columns:
                    isg_df = gsel[["Dia","Turno","ISG_est%"]].rename(columns={"ISG_est%":"Valor"})
                    isg_df["Serie"]="ISG estimado"
                    rule = alt.Chart(pd.DataFrame({"y":[Proc.ISG_SET]})).mark_rule(strokeDash=[4,4]).encode(y="y:Q")
                    ch_isg = alt.Chart(isg_df.dropna()).mark_line(point=True).encode(
                        x=alt.X("Dia:Q", title=f"Dia ({_N[m_sel]} {y_sel})"),
                        y=alt.Y("Valor:Q", title="ISG (%)", scale=alt.Scale(domain=[0,100])),
                        color="Serie:N",
                        tooltip=["Dia","Turno","Valor"]
                    )
                    st.altair_chart(rule + ch_isg, use_container_width=True)

                if "P80_real%" in gsel.columns:
                    p80_df = gsel[["Dia","Turno","P80_real%"]].rename(columns={"P80_real%":"Valor"})
                    p80_df["Serie"]="P80 real"
                    rule80 = alt.Chart(pd.DataFrame({"y":[Proc.OBJ_P80]})).mark_rule(strokeDash=[4,4]).encode(y="y:Q")
                    ch_p80 = alt.Chart(p80_df.dropna()).mark_line(point=True).encode(
                        x=alt.X("Dia:Q", title=f"Dia ({_N[m_sel]} {y_sel})"),
                        y=alt.Y("Valor:Q", title="P80 (% pasante)", scale=alt.Scale(domain=[0,100])),
                        color="Serie:N",
                        tooltip=["Dia","Turno","Valor"]
                    )
                    st.altair_chart(rule80 + ch_p80, use_container_width=True)

# ----------------------------------------------- Termino de Modulo
with tabs[2]:
    st.subheader("Ingreso — Termino de Modulo")
    df_hist = read_csv_df(csv_hist, HIST_HEAD, kind="hist")

    c1,c2,c3 = st.columns(3)
    fecha_mod = c1.text_input("Fecha y hora (dd/mm/aaaa HH:MM)", value=datetime.now().strftime("%d/%m/%Y %H:%M"), key="tm_fecha_hora")
    ciclo_m   = c2.text_input("Ciclo", key="tm_ciclo")
    modulo_m  = c3.text_input("Modulo", key="tm_modulo")

    c1,c2,c3 = st.columns(3)
    ton_total = c1.number_input("Tonelaje total (t)", min_value=0.0, value=0.0, key="tm_ton")
    acido_m3  = c2.number_input("Acido total (m3)", min_value=0.0, value=0.0, key="tm_acid_m3")
    agua_m3   = c3.number_input("Agua total (m3)", min_value=0.0, value=0.0, key="tm_agua_m3")

    c1,c2,c3 = st.columns(3)
    p80_real_m = c1.number_input("P80 real (%)", min_value=0.0, max_value=100.0, value=0.0, key="tm_p80r")
    finos_m    = c2.number_input("Finos #100 (%)", min_value=0.0, max_value=100.0, value=0.0, key="tm_finos")
    isg_real_m = c3.number_input("Sulfatacion real (%)", min_value=0.0, max_value=100.0, value=0.0, key="tm_isgr")
    obs_m = st.text_input("Observaciones", value="", key="tm_obs")

    colf1, colf2 = st.columns(2)
    btn_fetch = colf1.button("Cargar estimados desde historico", key="tm_fetch")
    btn_save  = colf2.button("Guardar modulo", disabled=solo_lectura, key="tm_save")

    isg_est_text = ""
    perd_text = ""
    if btn_fetch:
        try:
            if not df_hist.empty:
                m = df_hist[(df_hist["Ciclo"].astype(str).str.strip()==ciclo_m.strip()) &
                            (df_hist["Modulo"].astype(str).str.strip()==modulo_m.strip())]
                if not m.empty:
                    isg_est_text = str(m.iloc[-1]["ISG_est%"])
                    perd_text = f"Perdida (kgCu/h): {m.iloc[-1]['Perd_kgCu_h']}"
                    st.info(f"ISG estimado modulo: {isg_est_text}% | {perd_text}")
                else:
                    st.warning("No se encontro ciclo/modulo en historico.")
            else:
                st.warning("Historico vacio.")
        except Exception as e:
            st.error(f"Error: {e}")

    if btn_save and not solo_lectura:
        ensure_headers(csv_mod, MOD_HEAD)
        dfm = read_csv_df(csv_mod, MOD_HEAD, kind="mod")
        row = [fecha_mod, ciclo_m, modulo_m, f"{ton_total:g}", f"{acido_m3:g}", f"{agua_m3:g}",
               f"{p80_real_m:g}", f"{finos_m:g}", perd_text, f"{isg_est_text}", f"{isg_real_m:g}", obs_m]
        dfm.loc[len(dfm)] = row
        write_csv_df(csv_mod, dfm)
        st.success("Modulo guardado.")

# ----------------------------------------------- Hist. Modulos
with tabs[3]:
    st.subheader("Historico — Termino de Modulos")
    dfm = read_csv_df(csv_mod, MOD_HEAD, kind="mod")
    if dfm.empty:
        st.info("Sin registros.")
    else:
        c1,c2,c3,c4 = st.columns(4)
        f_ciclo = c1.text_input("Ciclo", key="hm_ciclo")
        f_mod   = c2.text_input("Modulo", key="hm_modulo")
        f_desde = c3.text_input("Desde dd/mm/aaaa", key="hm_desde")
        f_hasta = c4.text_input("Hasta dd/mm/aaaa", key="hm_hasta")

        if f_ciclo: dfm = dfm[dfm["Ciclo"].astype(str).str.contains(f_ciclo, case=False, na=False)]
        if f_mod:   dfm = dfm[dfm["Modulo"].astype(str).str.contains(f_mod, case=False, na=False)]
        if f_desde:
            ddes = csv_datetime(f_desde)
            if ddes: dfm = dfm[dfm["FechaHora"].apply(lambda x: (csv_datetime(str(x)) or datetime(1900,1,1))>=ddes)]
        if f_hasta:
            dhas = csv_datetime(f_hasta)
            if dhas: dfm = dfm[dfm["FechaHora"].apply(lambda x: (csv_datetime(str(x)) or datetime(9999,1,1))<=dhas)]

        if not isinstance(dfm, pd.DataFrame):
            st.error("Datos de módulos no válidos.")
        else:
            edited = st.data_editor(dfm, num_rows="dynamic" if not solo_lectura else "fixed", disabled=solo_lectura, key="hm_editor")
            c1,c2 = st.columns(2)
            b_exp = c1.button("Exportar Excel", key="hm_export")
            b_apply = c2.button("Aplicar cambios al CSV", disabled=solo_lectura, key="hm_apply")

            if b_exp:
                path = export_rows_to_excel("historicos_modulos", list(edited.columns), edited.values.tolist())
                open_path_hint(path)

            if b_apply and not solo_lectura:
                write_csv_df(csv_mod, edited)
                st.success("Cambios aplicados.")

# ----------------------------------------------- Hist. Sulfatacion
with tabs[4]:
    st.subheader("Historico — Sulfatacion (estimada vs real)")
    ensure_headers(csv_sulf, SULF_HEAD)
    dfs = read_csv_df(csv_sulf, SULF_HEAD, kind="sulf").copy()

    text_cols = ["FechaHora", "Ciclo", "Modulo", "Fuente", "Observaciones"]
    num_cols  = ["ISG_est%", "ISG_real%"]

    for c in text_cols:
        if c in dfs.columns:
            dfs[c] = dfs[c].astype(str).replace("nan", "").fillna("")
    for c in num_cols:
        if c in dfs.columns:
            dfs[c] = pd.to_numeric(dfs[c], errors="coerce")

    if dfs.empty:
        st.info("Sin registros.")
    else:
        col_cfg = {
            "FechaHora":   st.column_config.TextColumn("FechaHora",   disabled=True),
            "Ciclo":       st.column_config.TextColumn("Ciclo",       disabled=True),
            "Modulo":      st.column_config.TextColumn("Modulo",      disabled=True),
            "ISG_est%":    st.column_config.NumberColumn("ISG_est%",  disabled=True),
            "ISG_real%":   st.column_config.NumberColumn("ISG_real%", help="Ingrese resultado de laboratorio"),
            "Fuente":      st.column_config.TextColumn("Fuente",      disabled=True),
            "Observaciones": st.column_config.TextColumn("Observaciones", help="Notas / comentarios"),
        }
        edited = st.data_editor(
            dfs, num_rows="dynamic" if not solo_lectura else "fixed",
            disabled=solo_lectura, column_config=col_cfg, key="sulf_editor"
        )

        c1, c2 = st.columns(2)
        b_exp   = c1.button("Exportar Excel", key="hs_export")
        b_apply = c2.button("Aplicar cambios al CSV", disabled=solo_lectura, key="hs_apply")

        if b_exp:
            path = export_rows_to_excel("historicos_sulfatacion", list(edited.columns), edited.values.tolist())
            open_path_hint(path)

        if b_apply and not solo_lectura:
            for c in text_cols:
                if c in edited.columns:
                    edited[c] = edited[c].astype(str).replace("nan", "").fillna("")
            for c in num_cols:
                if c in edited.columns:
                    edited[c] = pd.to_numeric(edited[c], errors="coerce")
            write_csv_df(csv_sulf, edited)
            st.success("Cambios aplicados.")

        # --------- Gráfico mensual por módulo ----------
        dfs2 = edited.copy()
        dfs2["__dt"] = dfs2["FechaHora"].apply(lambda x: csv_datetime(str(x)))
        dfs2 = dfs2.dropna(subset=["__dt"])
        if dfs2.empty:
            st.info("Aun no hay fechas validas para graficar.")
        else:
            dfs2["Año"] = dfs2["__dt"].dt.year
            dfs2["Mes"] = dfs2["__dt"].dt.month
            yms = sorted(dfs2[["Año","Mes"]].dropna().drop_duplicates().values.tolist())
            _N = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

            if yms:
                sel_idx = len(yms) - 1
                sel_ym = st.selectbox(
                    "Mes",
                    options=[tuple(x) for x in yms],
                    index=sel_idx,
                    format_func=lambda t: f"{_N[t[1]]} {t[0]}",
                    key="sulf_mes"
                )
                y_sel, m_sel = sel_ym
                dfm = dfs2[(dfs2["Año"]==y_sel) & (dfs2["Mes"]==m_sel)].copy()
            else:
                st.info("No hay meses disponibles.")
                dfm = dfs2.iloc[0:0].copy()

            for c in ["ISG_est%", "ISG_real%"]:
                if c in dfm.columns:
                    dfm[c] = pd.to_numeric(dfm[c], errors="coerce")

            agg_mode = st.radio(
                "Si hay multiples lecturas por modulo en el mes:",
                ["Ultimo valor", "Promedio mensual"],
                horizontal=True,
                key="sulf_aggmode"
            )
            if agg_mode == "Ultimo valor":
                dfm = dfm.sort_values("__dt").groupby("Modulo", as_index=False).tail(1)
            else:
                dfm = dfm.groupby("Modulo", as_index=False)[["ISG_est%", "ISG_real%"]].mean()

            plot_df = dfm[["Modulo","ISG_est%","ISG_real%"]].copy()
            plot_df["Modulo"] = plot_df["Modulo"].astype(str)
            plot_df = plot_df.melt(id_vars="Modulo",
                                   value_vars=["ISG_est%","ISG_real%"],
                                   var_name="Serie", value_name="ISG").dropna()

            st.markdown("### ISG mensual por modulo")
            if plot_df.empty:
                st.info("No hay datos validos para graficar en el mes seleccionado.")
            else:
                rule = alt.Chart(pd.DataFrame({"target":[Proc.ISG_SET]})).mark_rule(strokeDash=[4,4]).encode(
                    y=alt.Y("target:Q", title="ISG (%)", scale=alt.Scale(domain=[0,100]))
                )
                chart = alt.Chart(plot_df).mark_circle(size=90).encode(
                    x=alt.X("Modulo:N", title="Modulo", sort="ascending",
                            axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("ISG:Q", title="ISG (%)", scale=alt.Scale(domain=[0,100])),
                    color=alt.Color("Serie:N", title="Serie", scale=alt.Scale(scheme="tableau10")),
                    tooltip=["Modulo","Serie","ISG"]
                )
                line = alt.Chart(plot_df).mark_line().encode(
                    x=alt.X("Modulo:N", axis=alt.Axis(labelAngle=0)),
                    y="ISG:Q", color="Serie:N"
                )
                st.altair_chart(rule + chart + line, use_container_width=True)

# ----------------------------------------------- Simulador / Optimizacion
with tabs[5]:
    st.subheader("Recomendador de setpoints")
    st.caption("Incluye CO3, NO3 y finos. Penalizacion ajustable si no hay modelo ML.")

    c1,c2,c3,c4 = st.columns(4)
    sim_tph  = c1.number_input("TPH (t/h)", min_value=0.0, value=1900.0, key="sim_tph")
    sim_cut  = c2.number_input("CuT (%)",   min_value=0.0, max_value=5.0, value=0.32, key="sim_cut")
    sim_cus  = c3.number_input("Cu soluble (%)", min_value=0.0, max_value=5.0, value=0.12, key="sim_cus")
    sim_ral  = c4.number_input("RAL (g/L)", min_value=0.0, value=0.0, key="sim_ral")

    c1,c2,c3,c4 = st.columns(4)
    sim_acid_kgt = c1.number_input("Acido (kg/t)", min_value=0.0, value=11.0, key="sim_acid_kgt")
    sim_agua_m3h = c2.number_input("Agua m3/h", min_value=0.0, value=23.0, key="sim_agua_m3h")
    sim_rpm      = c3.number_input("RPM", min_value=0.1, value=7.0, key="sim_rpm")
    sim_h        = c4.number_input("Humedad inicial (%)", min_value=0.0, max_value=100.0, value=6.0, key="sim_h")

    c1,c2,c3 = st.columns(3)
    sim_co3   = c1.number_input("CO3 (%)", min_value=0.0, value=0.54, key="sim_co3")
    sim_no3   = c2.number_input("NO3 (g/L)", min_value=0.0, value=0.09, key="sim_no3")
    sim_finos = c3.number_input("Finos #100 (%)", min_value=0.0, max_value=100.0, value=11.0, key="sim_finos")

    with st.expander("Parametros quimicos"):
        st.caption("Penalizacion de acido cuando **no** hay modelo ML (referenciales).")
        k_co3  = st.number_input("kg/t extra por 1% CO3", min_value=0.0, value=8.0, step=0.5, key="sim_kco3")
        k_no3  = st.number_input("kg/t extra por 1 g/L NO3", min_value=0.0, value=0.6, step=0.1, key="sim_kno3")
        k_fino = st.number_input("kg/t extra por cada punto de Finos sobre 25%", min_value=0.0, value=0.2, step=0.05, key="sim_kf")

    # Humedad simulada: agua_m3h + ácido kg/t (sin flujos de ácido en m3/h)
    sim_hbal = humedad_balance(sim_h, sim_tph, sim_agua_m3h, 0.0, 0.0, sim_acid_kgt)
    sim_tres = residence_time_min(sim_rpm)

    penalty_kgt = k_co3*sim_co3 + k_no3*sim_no3 + k_fino*max(0.0, sim_finos-25.0)
    sim_isg_formula, need_kgph_s, avail_kgph_s, diff_kgph_s = calc_isg_formula(
        sim_cut, sim_cus, sim_tph, sim_agua_m3h, sim_ral, sim_acid_kgt, extra_kgt=penalty_kgt
    )

    if model_isg is not None:
        try:
            fmap = {"humedad_balance":sim_hbal,"hum%":sim_hbal,"cut%":sim_cut,"cus%":sim_cus,
                    "no3":sim_no3,"co3":sim_co3,"tph":sim_tph,"acid_kgt":sim_acid_kgt,
                    "agua_kgt_total":(sim_agua_m3h*Proc.RHO_W*1000.0/max(sim_tph,1e-9) if sim_tph>0 else 0.0),
                    "rpm":sim_rpm,"t_res_min":sim_tres,"ral_gpl":sim_ral,"finos_%":sim_finos}
            v = [fmap.get((f if isinstance(f,str) else str(f)).lower(),0.0) for f in (isg_feats or [])]
            if not v: v = [sim_hbal, sim_cut, sim_cus, sim_no3, sim_co3, sim_tph,
                           sim_acid_kgt, fmap["agua_kgt_total"], sim_rpm, sim_tres, sim_ral, sim_finos]
            sim_isg = float(model_isg.predict([v])[0]); sim_isg = clamp(sim_isg,0,100)
            fuente = "Modelo ML"
        except Exception:
            sim_isg = sim_isg_formula; fuente="Formula (fallback)"
    else:
        sim_isg = sim_isg_formula; fuente="Formula (+penalizaciones)"

    st.info(
        f"ISG simulado: **{sim_isg:.1f}%** · {fuente} | "
        f"Requerido: {pretty_kg(need_kgph_s)} kg/h, Disponible: {pretty_kg(avail_kgph_s)} kg/h, "
        f"Δ {pretty_kg(diff_kgph_s)} kg/h (penalizacion ≈ {penalty_kgt:.2f} kg/t)."
    )

    st.markdown("#### Recomendador de acido (kg/t)")
    if sim_tph<=0 or sim_cut<=0:
        st.warning("Define TPH y CuT para recomendar.")
    else:
        need_target = Proc.MRATIO * (sim_tph*1000*(max(sim_cus, sim_cut*0.6)/100.0))
        # Ácido aportado por RAL (si hay): agua_m3h * g/L
        vol_kgph = sim_agua_m3h*sim_ral if (sim_agua_m3h>0 and sim_ral>0) else 0.0
        req_kgt = max(0.0, (need_target + penalty_kgt*sim_tph - vol_kgph)/max(sim_tph,1e-9))
        st.success(f"Para ISG≈{Proc.ISG_SET:.0f}% el piso recomendado es **{req_kgt:.2f} kg/t** (con RAL y penalizaciones actuales).")


