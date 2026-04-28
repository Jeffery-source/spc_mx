import uuid
from datetime import datetime
from sqlalchemy import text
import hashlib
from sqlalchemy import text

def mark_processed(engine, file_name, file_path, file_hash):
    sql = text("""
        INSERT INTO file_process_log (
            id, file_name, file_path, file_hash, status, process_time
        )
        VALUES (
            UUID(), :file_name, :file_path, :file_hash, 'SUCCESS', NOW()
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "file_name": file_name,
            "file_path": file_path,
            "file_hash": file_hash
        })

def is_processed(engine, file_hash):
    sql = text("""
        SELECT 1 FROM file_process_log
        WHERE file_hash = :file_hash
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, {"file_hash": file_hash}).fetchone()
        return result is not None

def get_file_hash(file_path):
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def build_datetime(date_str, time_str):
    if not date_str or not time_str:
        return None

    # 1️⃣ 解析日期（你的格式：2-Apr-26）
    date_part = datetime.strptime(date_str.strip(), "%d-%b-%y").date()

    # 2️⃣ 解析时间（14:46:53）
    time_part = datetime.strptime(time_str.strip(), "%H:%M:%S").time()

    # 3️⃣ 合并
    return datetime.combine(date_part, time_part)


def parse_csv(file_path):
    encodings = ["utf-8", "utf-8-sig", "gbk", "latin1"]
    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f:
                lines = [line.strip().split(",") for line in f.readlines()]
                print(f"✅ 使用编码: {enc}")
                return lines
        except UnicodeDecodeError:
            continue

    raise Exception("❌ 所有编码尝试失败")


def extract_header(lines):
    header = {}

    for i, row in enumerate(lines):

        # 🟦 找到 Measurement Plan 行
        if "Measurement Plan" in row:
            next_row = lines[i + 1] if i + 1 < len(lines) else []

            header["measurement_plan"] = next_row[1] if len(next_row) > 1 else None
             # 📅 日期（第5列）
            header["measure_date"] = next_row[3] if len(next_row) > 3 else None
            header["order_no"] = next_row[5] if len(next_row) > 5 else None

        # 🟩 找到 Drawing No 行
        if "Drawing No." in row:
            next_row = lines[i + 1] if i + 1 < len(lines) else []

            header["drawing_no"] = next_row[1] if len(next_row) > 1 else None
            header["part_no"] = next_row[5] if len(next_row) > 5 else None
            header["measure_time"] = next_row[3] if len(next_row) > 3 else None

    return header


def extract_detail(lines, report_id):
    start = False
    rows = []

    for row in lines:
        if row and row[0] == "Characteristic":
            start = True
            continue

        if start and len(row) >= 6 and row[0]:
            rows.append({
                "id": str(uuid.uuid4()),
                "report_id": report_id,
                "feature_code": row[0],
                "actual": float(row[1]) if row[1] else None,
                "deviation": float(row[5]) if row[5] else None
            })

    return rows


def extract_feature_standard(lines,measuredate):
    start = False
    rows = []

    for row in lines:

        # 🟦 找到表头
        if row and row[0] == "Characteristic":
            start = True
            continue

        # 🟩 解析数据区
        if start and len(row) >= 5 and row[0]:

            rows.append({
                "id": str(uuid.uuid4()), 
                "feature_code": row[0],
                "nominal": float(row[2]) if row[2] else None,
                "upper_tol": float(row[3]) if row[3] else None,
                "lower_tol": float(row[4]) if row[4] else None,
                "revision":measuredate,
                "effective_date":datetime.now()

            })

    return rows


def insert_feature_standard(conn, df):

    sql=text("""
    INSERT INTO feature_standard (
        id,
        feature_code,
        nominal,
        upper_tol,
        lower_tol,
        revision,
        effective_date
    )
    VALUES (
        :id,
        :feature_code,
        :nominal,
        :upper_tol,
        :lower_tol,
        :revision,
        :effective_date
    )
    ON DUPLICATE KEY UPDATE
        nominal = VALUES(nominal),
        upper_tol = VALUES(upper_tol),
        lower_tol = VALUES(lower_tol),
        revision = VALUES(revision),
        effective_date = VALUES(effective_date)
    """)

    for _, row in df.iterrows():
        conn.execute(sql, {
            "id": row["id"],
            "feature_code": row["feature_code"],
            "nominal": row["nominal"],
            "upper_tol": row["upper_tol"],
            "lower_tol": row["lower_tol"],
            "revision": row["revision"],
            "effective_date": row["effective_date"]
        })