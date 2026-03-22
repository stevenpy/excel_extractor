import os
import time
import json
import base64
import re
import unicodedata
from io import BytesIO

import psycopg
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
import zipfile


DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    return psycopg.connect(DATABASE_URL, sslmode="require")


def normalize_text(value):
    text = str(value or "").strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


PRODUCT_HEADER_CANDIDATES = [
    "type_de_fournitures_services",
    "type_fournitures_services",
    "fournitures_services",
    "fourniture",
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


def normalize_key(value):
    text = normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def load_workbook_safe(content: bytes):
    try:
        return load_workbook(filename=BytesIO(content), data_only=True)
    except (InvalidFileException, zipfile.BadZipFile):
        raise RuntimeError("Unsupported Excel format. The uploaded file is not a valid .xlsx file.")


def choose_best_sheet(wb):
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


def detect_header_row(rows):
    best_score = -1
    best_row_idx = None
    best_mapping = {}

    for row_idx, row in enumerate(rows[:40]):
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
                score += 5
            if len(norm) > 2:
                score += 1

        if score > best_score:
            best_score = score
            best_row_idx = row_idx
            best_mapping = mapping

    if best_score < 5:
        return None, {}

    return best_row_idx, best_mapping


def find_column_by_candidates(header_map, candidates):
    for candidate in candidates:
        for col_idx, norm_name in header_map.items():
            if norm_name == candidate:
                return col_idx
    return None


def fallback_detect_product_col(data_rows):
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


def parse_client_xlsx_bytes(content: bytes):
    wb = load_workbook_safe(content)
    sheet_name, rows = choose_best_sheet(wb)

    if not rows:
        raise RuntimeError("Empty workbook")

    header_row_idx, header_map = detect_header_row(rows)
    parsed_rows = []

    if header_row_idx is not None:
        data_rows = rows[header_row_idx + 1 :]
        product_col = find_column_by_candidates(header_map, PRODUCT_HEADER_CANDIDATES)
        qty_col = find_column_by_candidates(header_map, QTY_HEADER_CANDIDATES)

        if product_col is None:
            product_col = fallback_detect_product_col(data_rows)

        for idx, row in enumerate(data_rows, start=1):
            product_value = row[product_col] if product_col is not None and product_col < len(row) else None
            product_label = normalize_text(product_value)

            if not product_label:
                continue

            low = product_label.lower()
            if "type de fournitures" in low:
                continue
            if "reference fournisseur" in low:
                continue
            if low == "pu":
                continue
            if len(product_label) < 3:
                continue

            quantity = 1
            if qty_col is not None and qty_col < len(row):
                raw_qty = row[qty_col]
                txt_qty = normalize_text(raw_qty).replace(",", ".")
                try:
                    q = float(txt_qty)
                    if q > 0:
                        quantity = q
                except Exception:
                    quantity = 1

            parsed_rows.append({
                "request_row_number": idx,
                "product_label": product_label,
                "product_label_clean": normalize_text(product_label).upper(),
                "quantity": quantity,
            })

        return parsed_rows

    data_rows = rows
    product_col = fallback_detect_product_col(data_rows)

    for idx, row in enumerate(data_rows, start=1):
        product_value = row[product_col] if product_col is not None and product_col < len(row) else None
        product_label = normalize_text(product_value)

        if not product_label:
            continue
        if len(product_label) < 3:
            continue

        parsed_rows.append({
            "request_row_number": idx,
            "product_label": product_label,
            "product_label_clean": normalize_text(product_label).upper(),
            "quantity": 1,
        })

    return parsed_rows


def parse_email_body(body: str):
    def clean_line(line):
        return re.sub(r"\s+", " ", str(line or "")).strip()

    lines = [clean_line(x) for x in str(body or "").splitlines()]
    lines = [x for x in lines if x]

    out = []
    idx = 0

    for line in lines:
        low = line.lower()
        if low in {"bonjour", "bonsoir", "merci", "cordialement"}:
            continue
        if low.endswith(":"):
            continue
        if len(line) < 3:
            continue

        idx += 1
        out.append({
            "request_row_number": idx,
            "product_label": line,
            "product_label_clean": normalize_text(line).upper(),
            "quantity": 1,
        })

    return out


def fetch_next_job():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    with next_job as (
                      select id
                      from public.quote_jobs
                      where status = 'queued'
                      order by created_at
                      for update skip locked
                      limit 1
                    )
                    update public.quote_jobs q
                    set status = 'processing',
                        started_at = now(),
                        attempt_count = attempt_count + 1
                    from next_job
                    where q.id = next_job.id
                    returning q.id, q.supplier_id, q.input_type, q.payload_json;
                    """
                )
                row = cur.fetchone()

        return row
    finally:
        conn.close()


def mark_done(job_id: int, result_payload: dict):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.quote_jobs
                    set status = 'done',
                        finished_at = now(),
                        payload_json = %s
                    where id = %s
                    """,
                    (json.dumps(result_payload), job_id)
                )
    finally:
        conn.close()


def mark_failed(job_id: int, error_message: str):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.quote_jobs
                    set status = 'failed',
                        finished_at = now(),
                        error_message = %s
                    where id = %s
                    """,
                    (error_message[:4000], job_id)
                )
    finally:
        conn.close()


def process_job(job_row):
    job_id, supplier_id, input_type, payload_json = job_row

    payload = payload_json or {}

    if input_type == "xlsx":
        file_b64 = payload.get("file_base64")
        if not file_b64:
            raise RuntimeError("Missing file_base64 in payload_json")

        content = base64.b64decode(file_b64)
        parsed_rows = parse_client_xlsx_bytes(content)

        result = {
            **payload,
            "supplier_id": supplier_id,
            "parsed_rows": parsed_rows,
            "rows_parsed": len(parsed_rows),
        }
        mark_done(job_id, result)
        return

    if input_type == "email_body":
        body = payload.get("email_body", "")
        parsed_rows = parse_email_body(body)

        result = {
            **payload,
            "supplier_id": supplier_id,
            "parsed_rows": parsed_rows,
            "rows_parsed": len(parsed_rows),
        }
        mark_done(job_id, result)
        return

    raise RuntimeError(f"Unsupported input_type: {input_type}")


def main():
    while True:
        job = fetch_next_job()

        if not job:
            time.sleep(2)
            continue

        job_id = job[0]

        try:
            process_job(job)
            print(f"Job {job_id} done")
        except Exception as e:
            print(f"Job {job_id} failed: {e}")
            mark_failed(job_id, str(e))


if __name__ == "__main__":
    main()