from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from openpyxl import load_workbook
import psycopg
import os
import re
import unicodedata
import tempfile

app = FastAPI()

API_TOKEN = os.getenv("IMPORT_API_TOKEN", "")

def slugify(value: str) -> str:
    value = (value or "").strip()
    value = unicodedata.normalize("NFD", value)
    value = "".join(c for c in value if unicodedata.category(c) != "Mn")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    return value or "field"

def unique_columns(headers):
    used = set()
    result = []
    for i, h in enumerate(headers):
        base = slugify(str(h) if h is not None else f"column_{i+1}")
        if base and base[0].isdigit():
            base = f"c_{base}"
        elif not base:
            base = f"column_{i+1}"

        name = base
        n = 2
        while name in used:
            name = f"{base}_{n}"
            n += 1
        used.add(name)
        result.append(name)
    return result

def table_name_from_filename(filename: str) -> str:
    base = filename.rsplit(".", 1)[0]
    return f'import_{slugify(base)}'

def get_conn():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/import-xlsx")
async def import_xlsx(
    file: UploadFile = File(...),
    x_api_token: str | None = Header(default=None),
):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        wb = load_workbook(tmp_path, read_only=True, data_only=True)
        ws = wb.active

        rows = ws.iter_rows(values_only=True)

        header_row = next(rows, None)
        if not header_row:
            raise HTTPException(status_code=400, detail="Empty file")

        original_headers = [str(h) if h is not None else "" for h in header_row]
        normalized_headers = unique_columns(original_headers)
        table_name = table_name_from_filename(file.filename)

        columns_sql = ",\n  ".join([f'"{c}" text' for c in normalized_headers])

        create_sql = f'''
        create table if not exists public."{table_name}" (
          id bigserial primary key,
          imported_at timestamptz default now(),
          source_file text,
          row_number bigint,
          {columns_sql}
        );
        '''

        batch_size = 1000
        total_rows = 0
        batch = []

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)

                for idx, row in enumerate(rows, start=1):
                    values = [file.filename, idx]

                    for cell in row[:len(normalized_headers)]:
                        values.append(None if cell is None else str(cell))

                    while len(values) < 2 + len(normalized_headers):
                        values.append(None)

                    batch.append(values)
                    total_rows += 1

                    if len(batch) >= batch_size:
                        insert_batch(cur, table_name, normalized_headers, batch)
                        batch = []

                if batch:
                    insert_batch(cur, table_name, normalized_headers, batch)

            conn.commit()

        return {
            "success": True,
            "table_name": table_name,
            "rows_imported": total_rows,
            "source_file": file.filename,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def insert_batch(cur, table_name, headers, batch):
    col_sql = ", ".join([f'"{c}"' for c in headers])

    placeholders_groups = []
    params = []

    for row in batch:
        start = len(params) + 1
        placeholders = [f"${i}" for i in range(start, start + len(row))]
        placeholders_groups.append(f"({', '.join(placeholders)})")
        params.extend(row)

    sql = f'''
    insert into public."{table_name}" (
      source_file,
      row_number,
      {col_sql}
    ) values
    {", ".join(placeholders_groups)};
    '''

    cur.execute(sql, params)