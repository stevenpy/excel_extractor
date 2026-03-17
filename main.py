from fastapi import FastAPI, File, UploadFile, Header, HTTPException
from fastapi.responses import JSONResponse
from openpyxl import load_workbook
from io import BytesIO
import os
import re
import unicodedata
from typing import Any

app = FastAPI()

API_TOKEN = os.getenv("IMPORT_API_TOKEN", "")


def check_token(x_api_token: str | None):
    if not API_TOKEN:
        raise HTTPException(status_code=500, detail="Server token not configured")
    if x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_key(value: Any) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


PRODUCT_HEADER_CANDIDATES = [
    "type_de_fournitures_services",
    "type_fournitures_services",
    "fournitures_services",
    "produit",
    "product",
    "article",
    "designation",
    "libelle",
    "description",
    "item",
    "nom_produit",
    "prestation",
    "service",
]

QTY_HEADER_CANDIDATES = [
    "quantite",
    "quantity",
    "qty",
    "qte",
    "qte_",
    "nombre",
    "nb",
    "volume",
]


def detect_header_row(rows: list[list[Any]]) -> tuple[int | None, dict[int, str]]:
    """
    Cherche une ligne de header probable.
    Retourne:
      - index de ligne
      - mapping {col_index: normalized_header}
    """
    best_score = -1
    best_row_idx = None
    best_mapping = {}

    for row_idx, row in enumerate(rows[:40]):  # on regarde surtout le haut du fichier
        mapping = {}
        score = 0

        for col_idx, cell in enumerate(row):
            norm = normalize_key(cell)
            if not norm:
                continue
            mapping[col_idx] = norm

            if norm in PRODUCT_HEADER_CANDIDATES:
                score += 10
            if norm in QTY_HEADER_CANDIDATES:
                score += 4
            if len(norm) > 2:
                score += 1

        if score > best_score:
            best_score = score
            best_row_idx = row_idx
            best_mapping = mapping

    if best_score < 5:
        return None, {}

    return best_row_idx, best_mapping


def find_column_by_candidates(header_map: dict[int, str], candidates: list[str]) -> int | None:
    for candidate in candidates:
        for col_idx, norm_name in header_map.items():
            if norm_name == candidate:
                return col_idx
    return None


def fallback_detect_product_col(data_rows: list[list[Any]]) -> int | None:
    """
    Si on n'a pas de vrai header, on prend la colonne texte la plus "riche".
    """
    if not data_rows:
        return None

    max_cols = max(len(r) for r in data_rows)
    best_col = None
    best_score = -1

    for col_idx in range(max_cols):
        score = 0
        non_empty = 0
        long_text = 0

        for row in data_rows[:200]:
            val = row[col_idx] if col_idx < len(row) else None
            txt = normalize_text(val)
            if not txt:
                continue
            non_empty += 1
            if any(ch.isalpha() for ch in txt):
                score += 1
            if len(txt) >= 10:
                long_text += 1
                score += 2

        if non_empty >= 3 and long_text >= 3 and score > best_score:
            best_score = score
            best_col = col_idx

    return best_col


def fallback_detect_qty_col(data_rows: list[list[Any]], product_col: int | None) -> int | None:
    if not data_rows:
        return None

    max_cols = max(len(r) for r in data_rows)
    best_col = None
    best_score = -1

    for col_idx in range(max_cols):
        if col_idx == product_col:
            continue

        score = 0
        numeric_count = 0

        for row in data_rows[:200]:
            val = row[col_idx] if col_idx < len(row) else None
            if isinstance(val, (int, float)):
                numeric_count += 1
                score += 2
            else:
                txt = normalize_text(val)
                if re.fullmatch(r"\d+([.,]\d+)?", txt):
                    numeric_count += 1
                    score += 2

        if numeric_count >= 3 and score > best_score:
            best_score = score
            best_col = col_idx

    return best_col


def choose_best_sheet(wb) -> tuple[str, list[list[Any]]]:
    best_sheet_name = wb.sheetnames[0]
    best_rows = []
    best_score = -1

    for ws in wb.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        non_empty = 0
        text_cells = 0

        for row in rows[:300]:
            for cell in row:
                if cell is None or str(cell).strip() == "":
                    continue
                non_empty += 1
                if any(ch.isalpha() for ch in str(cell)):
                    text_cells += 1

        score = non_empty + text_cells * 2

        if score > best_score:
            best_score = score
            best_sheet_name = ws.title
            best_rows = rows

    return best_sheet_name, best_rows


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/parse-client-xlsx")
async def parse_client_xlsx(
    file: UploadFile = File(...),
    x_api_token: str | None = Header(default=None),
):
    check_token(x_api_token)

    content = await file.read()
    wb = load_workbook(filename=BytesIO(content), data_only=True)

    sheet_name, rows = choose_best_sheet(wb)
    if not rows:
        raise HTTPException(status_code=400, detail="Empty workbook")

    header_row_idx, header_map = detect_header_row(rows)

    parsed_rows = []

    if header_row_idx is not None:
        product_col = find_column_by_candidates(header_map, PRODUCT_HEADER_CANDIDATES)
        qty_col = find_column_by_candidates(header_map, QTY_HEADER_CANDIDATES)

        data_rows = rows[header_row_idx + 1 :]

        if product_col is None:
            product_col = fallback_detect_product_col(data_rows)

        if qty_col is None:
            qty_col = fallback_detect_qty_col(data_rows, product_col)

        for idx, row in enumerate(data_rows, start=1):
            product_value = row[product_col] if product_col is not None and product_col < len(row) else None
            product_label = normalize_text(product_value)

            if not product_label:
                continue

            low = product_label.lower()
            if "type de fournitures" in low or "reference fournisseur" in low or low == "pu":
                continue

            quantity = 1
            if qty_col is not None and qty_col < len(row):
                raw_qty = row[qty_col]
                txt_qty = normalize_text(raw_qty).replace(",", ".")
                try:
                    quantity = float(txt_qty)
                    if quantity <= 0:
                        quantity = 1
                except Exception:
                    quantity = 1

            parsed_rows.append({
                "request_row_number": idx,
                "product_label": product_label,
                "product_label_clean": normalize_text(product_label).upper(),
                "quantity": quantity,
            })

        return JSONResponse({
            "success": True,
            "sheet_name": sheet_name,
            "header_row_index": header_row_idx + 1,
            "product_col_index": product_col + 1 if product_col is not None else None,
            "qty_col_index": qty_col + 1 if qty_col is not None else None,
            "rows_parsed": len(parsed_rows),
            "rows": parsed_rows,
        })

    # Fallback sans header exploitable
    data_rows = rows
    product_col = fallback_detect_product_col(data_rows)
    qty_col = fallback_detect_qty_col(data_rows, product_col)

    for idx, row in enumerate(data_rows, start=1):
        product_value = row[product_col] if product_col is not None and product_col < len(row) else None
        product_label = normalize_text(product_value)

        if not product_label:
            continue

        quantity = 1
        if qty_col is not None and qty_col < len(row):
            raw_qty = row[qty_col]
            txt_qty = normalize_text(raw_qty).replace(",", ".")
            try:
                quantity = float(txt_qty)
                if quantity <= 0:
                    quantity = 1
            except Exception:
                quantity = 1

        parsed_rows.append({
            "request_row_number": idx,
            "product_label": product_label,
            "product_label_clean": normalize_text(product_label).upper(),
            "quantity": quantity,
        })

    return JSONResponse({
        "success": True,
        "sheet_name": sheet_name,
        "header_row_index": None,
        "product_col_index": product_col + 1 if product_col is not None else None,
        "qty_col_index": qty_col + 1 if qty_col is not None else None,
        "rows_parsed": len(parsed_rows),
        "rows": parsed_rows,
    })