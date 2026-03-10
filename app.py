import os
import re
import io
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# PDF readers
import pdfplumber

try:
    import fitz  # PyMuPDF (opcional, fallback)
    HAS_FITZ = True
except Exception:
    HAS_FITZ = False


# -----------------------------
# Config / Helpers
# -----------------------------
def normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_for_filename(s: str) -> str:
    s = normalize(s)
    s = re.sub(r'[<>:"/\\|?*\n\r\t]', " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_pdf_text(pdf_bytes: bytes) -> str:
    """Extrae texto del PDF. Primero pdfplumber; si queda muy vacío y hay PyMuPDF, usa fallback."""
    text = ""

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        parts = []
        for page in pdf.pages:
            t = page.extract_text() or ""
            parts.append(t)
        text = "\n".join(parts).strip()

    if HAS_FITZ and len(text) < 50:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            parts = []
            for page in doc:
                parts.append(page.get_text("text") or "")
            text2 = "\n".join(parts).strip()
            if len(text2) > len(text):
                text = text2
        except Exception:
            pass

    return text


# -----------------------------
# BL detection
# -----------------------------
BL_PATTERNS = [
    r"\bMBL\s*[:#]?\s*([A-Z0-9\-\/]{7,25})\b",
    r"\bHBL\s*[:#]?\s*([A-Z0-9\-\/]{7,25})\b",
    r"\bB\/L\s*[:#]?\s*([A-Z0-9\-\/]{7,25})\b",
    r"\bBL\s*[:#]?\s*([A-Z0-9\-\/]{7,25})\b",
    r"\b([A-Z]{3,5}[A-Z]{3}\d{6,10})\b",
]

CONTAINER_PATTERN = re.compile(r"\b([A-Z]{4}\d{7})\b")


def detect_bl(text: str):
    """
    Devuelve (bl_detectado, debug_tokens)
    """
    if not text:
        return None, []

    t = " ".join(text.split())
    debug_tokens = []

    for pat in BL_PATTERNS:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            if m.groups():
                cand = m.group(1)
            else:
                cand = m.group(0)

            cand = cand.upper().strip()
            cand = re.sub(r"[^\w\-\/]", "", cand)

            if len(cand) < 7:
                continue

            debug_tokens.append(cand)

    filtered = []
    for cand in debug_tokens:
        if CONTAINER_PATTERN.fullmatch(cand):
            continue
        filtered.append(cand)

    debug_tokens = filtered

    if not debug_tokens:
        return None, []

    def score(x):
        s = 0
        if re.fullmatch(r"[A-Z]{3,5}[A-Z]{3}\d{6,10}", x):
            s += 50
        if x.startswith(("MEDU", "HLCU", "MSCU", "ONEY", "COSU", "MAEU", "OOLU", "CMAU")):
            s += 30
        s += min(len(x), 30)
        if "/" in x or "-" in x:
            s -= 5
        return s

    debug_tokens_sorted = sorted(list(dict.fromkeys(debug_tokens)), key=score, reverse=True)
    best = debug_tokens_sorted[0]
    return best, debug_tokens_sorted[:25]


# -----------------------------
# Proveedor detection
# -----------------------------
PROVEEDOR_RULES = [
    ("EXOLGAN", [r"\bEXOLGAN\b", r"\bTERMINAL EXOLGAN\b"]),
    ("MSC", [r"\bMSC\b", r"\bMEDITERRANEAN SHIPPING\b"]),
    ("ONE", [r"\bOCEAN NETWORK EXPRESS\b", r"\bONE\b"]),
    ("HAPAG", [r"\bHAPAG\b", r"\bHAPAG-LLOYD\b", r"\bHLCU\b"]),
    ("MAERSK", [r"\bMAERSK\b", r"\bMAEU\b"]),
    ("CMA CGM", [r"\bCMA\b", r"\bCMA CGM\b", r"\bCMAU\b"]),
]


def detect_proveedor(text: str) -> str:
    if not text:
        return "SIN_PROVEEDOR"

    t = text.upper()

    for nombre, pats in PROVEEDOR_RULES:
        for p in pats:
            if re.search(p, t):
                return nombre

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines[:25]:
        up = ln.upper()
        if len(up) < 5:
            continue
        if re.search(r"[A-Z]", up) and not re.fullmatch(r"[A-Z0-9\-\./ ]{1,}", up):
            continue
        if any(word in up for word in ["S.A", "S.R.L", "SRL", "SA", "SOCIEDAD", "TERMINAL", "LOGIST", "SHIPPING", "EXPRESS", "TRANSPORT"]):
            return clean_for_filename(up)[:40]

    return "SIN_PROVEEDOR"


# -----------------------------
# Factura detection
# -----------------------------
def detect_numero_factura(text: str) -> str:
    if not text:
        return ""

    patterns = [
        r"(?:FACTURA|INVOICE|FACT\.)\s*(?:N[°ºo]\s*)?[:#]?\s*([A-Z0-9\-]{6,25})",
        r"\b([0-9]{4}-[0-9]{6,8})\b",
        r"\b([0-9]{10,14})\b",
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return ""


# -----------------------------
# Excel mapping
# -----------------------------
def build_mapping(df: pd.DataFrame):
    """
    Espera columnas:
    - operacion (o operación)
    - mbl / hbl / bl (cualquiera de estas, o una sola columna 'bl')
    Devuelve:
    - map_bl_to_op: dict {BL_NORMALIZADO: operacion}
    """
    cols = {c.lower().strip(): c for c in df.columns}

    op_col = None
    for key in ["operacion", "operación", "op", "nro_operacion", "numero_operacion", "número_operacion"]:
        if key in cols:
            op_col = cols[key]
            break

    if op_col is None:
        raise ValueError("No encuentro la columna de 'operacion' en el Excel.")

    bl_candidates = []
    for key in ["bl", "mbl", "hbl", "nro_bl", "numero_bl", "master", "house"]:
        if key in cols:
            bl_candidates.append(cols[key])

    if not bl_candidates:
        raise ValueError("No encuentro columna 'BL/MBL/HBL' en el Excel. Agregá una columna bl/mbl/hbl.")

    map_bl_to_op = {}
    for _, row in df.iterrows():
        op = normalize(row.get(op_col))
        if not op:
            continue

        for blc in bl_candidates:
            blv = normalize(row.get(blc)).upper()
            blv = re.sub(r"\s+", "", blv)
            blv = blv.replace("-", "").replace("/", "")
            if blv:
                map_bl_to_op[blv] = op

    return map_bl_to_op, op_col, bl_candidates


def normalize_bl_for_lookup(bl: str) -> str:
    bl = normalize(bl).upper()
    bl = re.sub(r"\s+", "", bl)
    bl = bl.replace("-", "").replace("/", "")
    return bl


# -----------------------------
# Main processing
# -----------------------------
def process_pdfs(pdf_files, mapping, output_dir: Path):
    results = []
    output_dir.mkdir(parents=True, exist_ok=True)

    for f in pdf_files:
        pdf_name = f.name
        pdf_bytes = f.read()

        text = read_pdf_text(pdf_bytes)
        proveedor = detect_proveedor(text)
        numero_factura = detect_numero_factura(text)
        bl_detectado, debug_tokens = detect_bl(text)

        operacion = ""
        status = "NO_ENCONTRADO"

        if bl_detectado:
            key = normalize_bl_for_lookup(bl_detectado)
            if key in mapping:
                operacion = mapping[key]
                status = "OK"
            else:
                status = "BL_DETECTADO_SIN_MATCH"

        if status == "OK":
            if numero_factura:
                nuevo_nombre = f"{operacion} - {proveedor} - {numero_factura} - {bl_detectado}.pdf"
            else:
                nuevo_nombre = f"{operacion} - {proveedor} - {bl_detectado}.pdf"
        else:
            if bl_detectado and numero_factura:
                nuevo_nombre = f"{status} - {proveedor} - {numero_factura} - {bl_detectado} - {pdf_name}"
            elif bl_detectado:
                nuevo_nombre = f"{status} - {proveedor} - {bl_detectado} - {pdf_name}"
            else:
                nuevo_nombre = f"{status} - {proveedor} - {pdf_name}"

        nuevo_nombre = clean_for_filename(nuevo_nombre)
        if not nuevo_nombre.lower().endswith(".pdf"):
            nuevo_nombre += ".pdf"

        out_path = output_dir / nuevo_nombre
        with open(out_path, "wb") as w:
            w.write(pdf_bytes)

        results.append({
            "pdf_original": pdf_name,
            "status": status,
            "operacion": operacion,
            "proveedor": proveedor,
            "numero_factura": numero_factura,
            "bl_detectado": bl_detectado or "",
            "nuevo_nombre": nuevo_nombre,
            "debug_tokens": " | ".join(debug_tokens) if debug_tokens else ""
        })

    return pd.DataFrame(results)


def make_zip(output_dir: Path, report_df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in output_dir.glob("*.pdf"):
            z.write(p, arcname=f"pdfs_renombrados/{p.name}")

        csv_bytes = report_df.to_csv(index=False).encode("utf-8-sig")
        z.writestr("reporte.csv", csv_bytes)

    return buf.getvalue()


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Renombrar Facturas por BL → Operación", layout="wide")
st.title("Renombrar Facturas PDF por HBL/MBL/BL → Operación + Proveedor + Factura")

st.write(
    "Subí el Excel con la columna de operación y alguna columna con BL/MBL/HBL, "
    "y luego subí los PDFs. El sistema detecta el BL, cruza contra el Excel, "
    "detecta proveedor, intenta detectar número de factura y renombra."
)

col1, col2 = st.columns(2)

with col1:
    excel_file = st.file_uploader("1) Excel de cruce (xlsx/xls/csv)", type=["xlsx", "xls", "csv"])

with col2:
    pdf_files = st.file_uploader("2) PDFs a renombrar", type=["pdf"], accept_multiple_files=True)

st.divider()

if excel_file and pdf_files:
    try:
        if excel_file.name.lower().endswith(".csv"):
            df_map = pd.read_csv(excel_file)
        else:
            df_map = pd.read_excel(excel_file)

        map_bl_to_op, op_col, bl_cols = build_mapping(df_map)

        st.success(f"Excel OK. Columna operación: {op_col} | Columnas BL detectadas: {', '.join(bl_cols)}")
        st.info(f"BLs cargados en el mapa: {len(map_bl_to_op):,}")

        run = st.button("Procesar PDFs")

        if run:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_dir = Path("output") / run_id

            report = process_pdfs(pdf_files, map_bl_to_op, out_dir)

            st.subheader("Resumen")
            st.write(f"Cantidad total de PDFs subidos: {len(pdf_files)}")
            st.write(f"Cantidad procesados: {len(report)}")
            st.write(f"Con match OK: {(report['status'] == 'OK').sum()}")
            st.write(f"Sin match: {(report['status'] != 'OK').sum()}")

            if "numero_factura" in report.columns:
                con_factura = report["numero_factura"].fillna("").astype(str).str.strip().ne("").sum()
                st.write(f"Con número de factura detectado: {con_factura}")

            st.subheader("Resultados")
            st.dataframe(report, use_container_width=True)

            zip_bytes = make_zip(out_dir, report)
            st.download_button(
                label="Descargar ZIP (PDFs renombrados + reporte CSV)",
                data=zip_bytes,
                file_name=f"renombrados_{run_id}.zip",
                mime="application/zip"
            )

            st.caption("Tip: si ves BL_DETECTADO_SIN_MATCH, el BL está en el PDF pero no existe en el Excel o está escrito distinto.")

    except Exception as e:
        st.error(f"Error procesando: {e}")
else:
    st.warning("Cargá el Excel y al menos 1 PDF para comenzar.")

st.divider()
with st.expander("Notas (si un PDF es escaneado)"):
    st.write(
        "- Si el PDF es escaneado (imagen) y no tiene texto, pdfplumber no puede leer BL.\n"
        "- En ese caso necesitás OCR. Si querés, después te lo adapto."
    )
