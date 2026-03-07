# app.py
# -*- coding: utf-8 -*-
import os
import re
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import requests
from flask import Flask, render_template, request, jsonify, Response
from dotenv import load_dotenv

from docx import Document
from pypdf import PdfReader

from openpyxl import load_workbook
import xlrd

from pptx import Presentation


load_dotenv()

APP_TITLE = "CHUPPY RAG CONVERTER"
HEADER_MODEL_LABEL = "Model : ChatGPT 5.2"

API_BASE = (os.getenv("DIFY_API_BASE") or "").strip().rstrip("/")
API_KEY = (os.getenv("DIFY_API_KEY") or "").strip()

DATASET_API_BASE = (os.getenv("DIFY_DATASET_API_BASE") or "").strip().rstrip("/")
DATASET_API_KEY = (os.getenv("DIFY_DATASET_API_KEY") or "").strip()

ALLOWED_EXTS = {
    ".txt", ".md", ".csv", ".json", ".log",
    ".html", ".xml", ".yml", ".yaml", ".ini", ".conf",
    ".py", ".js", ".css",
    ".docx", ".pdf",
    ".xlsx", ".xls", ".xlsm",
    ".ppt", ".pptx",
}

MAX_INPUT_CHARS = 180_000
DEFAULT_CHUNK_SEP = "***"
REQ_TIMEOUT_SEC = 300

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NOTICE_PATH = os.path.join(BASE_DIR, "notice.txt")


def ensure_notice_file() -> None:
    if os.path.exists(NOTICE_PATH):
        return
    try:
        with open(NOTICE_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass


def create_app():
    ensure_notice_file()

    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False

    def is_api_ready() -> bool:
        return bool(API_BASE and API_KEY)

    def is_dataset_api_ready() -> bool:
        # ナレッジAPIは専用設定を優先。未設定なら /chat-messages の設定を流用可能。
        return bool((DATASET_API_BASE and DATASET_API_KEY) or (API_BASE and API_KEY))

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            title=APP_TITLE,
            model_label=HEADER_MODEL_LABEL,
            api_ready=is_api_ready(),
        )

    @app.get("/auto")
    def auto_page():
        return render_template(
            "auto_rag.html",
            title=f"{APP_TITLE} (AUTO)",
            model_label=HEADER_MODEL_LABEL,
            api_ready=is_api_ready(),
            dataset_api_ready=is_dataset_api_ready(),
        )

    @app.get("/api/health")
    def api_health():
        return jsonify({
            "ok": True,
            "api_ready": is_api_ready(),
            "dataset_api_ready": is_dataset_api_ready(),
            "model_label": HEADER_MODEL_LABEL,
        })

    @app.get("/api/notice")
    def api_notice():
        ensure_notice_file()
        try:
            with open(NOTICE_PATH, "r", encoding="utf-8", errors="ignore") as f:
                txt = f.read()
        except Exception:
            return jsonify({"ok": False, "error": "notice.txt の読み取りに失敗しました。"}), 500

        if len(txt) > 50_000:
            txt = txt[:50_000] + "\n...(truncated)\n"

        return jsonify({"ok": True, "text": txt})

    @app.get("/api/datasets")
    def api_datasets():
        # Dify ナレッジベース一覧
        base, key = get_dataset_api_config()
        if not base or not key:
            return jsonify({"ok": False, "error": "ナレッジAPI設定が未完了です。.env を確認してください。"}), 500

        try:
            items = dify_list_datasets(base=base, key=key)
            return jsonify({"ok": True, "items": items})
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500

    @app.post("/api/scan")
    def api_scan():
        data = request.get_json(force=True) or {}
        in_dir = (data.get("input_dir") or "").strip()
        recursive = bool(data.get("recursive", True))

        if not in_dir or not os.path.isdir(in_dir):
            return jsonify({"ok": False, "error": "入力フォルダが存在しません。"}), 400

        files = list_files(in_dir, recursive=recursive)
        return jsonify({"ok": True, "count": len(files), "files": files})

    @app.post("/api/run")
    def api_run():
        if not is_api_ready():
            return jsonify({
                "ok": False,
                "error": "サーバー側API設定が未完了です。.env に DIFY_API_BASE / DIFY_API_KEY を設定してください。"
            }), 500

        data = request.get_json(force=True) or {}

        input_dir = (data.get("input_dir") or "").strip()
        output_dir = (data.get("output_dir") or "").strip()
        recursive = bool(data.get("recursive", True))

        user = (data.get("user") or "rag_converter").strip()
        knowledge_style = (data.get("knowledge_style") or "rag_markdown").strip()
        chunk_sep = (data.get("chunk_sep") or DEFAULT_CHUNK_SEP).strip() or DEFAULT_CHUNK_SEP

        overwrite = bool(data.get("overwrite", False))

        if not input_dir or not os.path.isdir(input_dir):
            return jsonify({"ok": False, "error": "入力フォルダが存在しません。"}), 400
        if not output_dir:
            return jsonify({"ok": False, "error": "出力フォルダが未指定です。"}), 400

        os.makedirs(output_dir, exist_ok=True)
        files = list_files(input_dir, recursive=recursive)

        def sse():
            yield sse_event("meta", {
                "title": APP_TITLE,
                "model": HEADER_MODEL_LABEL,
                "total": len(files),
                "overwrite": overwrite,
            })

            ok_count = 0
            ng_count = 0
            skip_count = 0

            for idx, relpath in enumerate(files, start=1):
                abspath = os.path.abspath(os.path.join(input_dir, relpath))
                yield sse_event("progress", {"index": idx, "total": len(files), "file": relpath})

                try:
                    out_path = make_output_path(output_dir, relpath)

                    if (not overwrite) and os.path.exists(out_path):
                        skip_count += 1
                        yield sse_event("skip_one", {"file": relpath, "out": os.path.relpath(out_path, output_dir)})
                        continue

                    raw_text, meta = extract_text(abspath, knowledge_style=knowledge_style)
                    meta["relpath"] = relpath
                    meta["abspath"] = abspath

                    if not raw_text.strip():
                        raise RuntimeError("抽出テキストが空でした。")

                    if len(raw_text) > MAX_INPUT_CHARS:
                        raw_text = raw_text[:MAX_INPUT_CHARS] + "\n...(truncated)\n"

                    answer = convert_via_dify_chat_messages_secure(
                        api_base=API_BASE,
                        api_key=API_KEY,
                        user=user,
                        source_path=relpath,
                        source_meta=meta,
                        text=raw_text,
                        knowledge_style=knowledge_style,
                        chunk_sep=chunk_sep,
                    )

                    md = wrap_markdown_with_source_meta(answer, meta)

                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
                        f.write(md)

                    ok_count += 1
                    yield sse_event("done_one", {"file": relpath, "out": os.path.relpath(out_path, output_dir)})

                except Exception as e:
                    ng_count += 1
                    yield sse_event("error_one", {"file": relpath, "error": safe_err(str(e))})

            yield sse_event("summary", {
                "ok": ok_count,
                "ng": ng_count,
                "skip": skip_count,
                "total": len(files),
                "overwrite": overwrite,
            })

        return Response(sse(), mimetype="text/event-stream")

    @app.post("/api/auto/run")
    def api_auto_run():
        if not is_api_ready():
            return jsonify({
                "ok": False,
                "error": "サーバー側API設定が未完了です。.env に DIFY_API_BASE / DIFY_API_KEY を設定してください。"
            }), 500

        base_ds, key_ds = get_dataset_api_config()
        if not base_ds or not key_ds:
            return jsonify({
                "ok": False,
                "error": "ナレッジAPI設定が未完了です。.env に DIFY_DATASET_API_BASE / DIFY_DATASET_API_KEY を設定してください。"
            }), 500

        data = request.get_json(force=True) or {}

        input_dir = (data.get("input_dir") or "").strip()
        output_dir = (data.get("output_dir") or "").strip()
        recursive = bool(data.get("recursive", True))

        dataset_id = (data.get("dataset_id") or "").strip()

        user = (data.get("user") or "rag_converter").strip()
        knowledge_style = (data.get("knowledge_style") or "rag_markdown").strip()
        chunk_sep = (data.get("chunk_sep") or DEFAULT_CHUNK_SEP).strip() or DEFAULT_CHUNK_SEP

        overwrite = bool(data.get("overwrite", False))

        if not dataset_id:
            return jsonify({"ok": False, "error": "ナレッジ（dataset_id）が未指定です。"}), 400

        if not input_dir or not os.path.isdir(input_dir):
            return jsonify({"ok": False, "error": "入力フォルダが存在しません。"}), 400
        if not output_dir:
            return jsonify({"ok": False, "error": "出力フォルダが未指定です。"}), 400

        os.makedirs(output_dir, exist_ok=True)
        files = list_files(input_dir, recursive=recursive)

        # dataset名をログ用に取る（失敗しても続行）
        dataset_name = None
        try:
            dataset_name = dify_get_dataset_name(base=base_ds, key=key_ds, dataset_id=dataset_id)
        except Exception:
            dataset_name = None

        def sse():
            yield sse_event("meta", {
                "title": APP_TITLE,
                "model": HEADER_MODEL_LABEL,
                "total": len(files),
                "overwrite": overwrite,
                "dataset_name": dataset_name,
            })

            ok_count = 0
            ng_count = 0
            skip_count = 0

            for idx, relpath in enumerate(files, start=1):
                abspath = os.path.abspath(os.path.join(input_dir, relpath))
                yield sse_event("progress", {"index": idx, "total": len(files), "file": relpath})

                try:
                    out_path = make_output_path(output_dir, relpath)

                    if (not overwrite) and os.path.exists(out_path):
                        skip_count += 1
                        yield sse_event("skip_one", {"file": relpath, "out": os.path.relpath(out_path, output_dir)})
                        continue

                    raw_text, meta = extract_text(abspath, knowledge_style=knowledge_style)
                    meta["relpath"] = relpath
                    meta["abspath"] = abspath

                    if not raw_text.strip():
                        raise RuntimeError("抽出テキストが空でした。")

                    if len(raw_text) > MAX_INPUT_CHARS:
                        raw_text = raw_text[:MAX_INPUT_CHARS] + "\n...(truncated)\n"

                    answer = convert_via_dify_chat_messages_secure(
                        api_base=API_BASE,
                        api_key=API_KEY,
                        user=user,
                        source_path=relpath,
                        source_meta=meta,
                        text=raw_text,
                        knowledge_style=knowledge_style,
                        chunk_sep=chunk_sep,
                    )

                    md = wrap_markdown_with_source_meta(answer, meta)

                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
                        f.write(md)

                    yield sse_event("done_one", {"file": relpath, "out": os.path.relpath(out_path, output_dir)})

                    # ---- ナレッジ登録 ----
                    doc_name = os.path.basename(out_path)
                    yield sse_event("dataset", {"message": f"ナレッジ登録開始: {doc_name}"})

                    # Markdownから分割長をざっくり推定（短い=小さく、長い=大きく）
                    max_tokens = estimate_max_tokens(md)

                    doc_form = "text_model"
                    if looks_hierarchical(md):
                        doc_form = "hierarchical_model"

                    resp = dify_create_doc_by_text(
                        base=base_ds,
                        key=key_ds,
                        dataset_id=dataset_id,
                        name=doc_name,
                        text=md,
                        separator=chunk_sep,
                        max_tokens=max_tokens,
                        doc_form=doc_form,
                    )

                    batch = resp.get("batch")
                    doc_id = (resp.get("document") or {}).get("id")

                    if doc_id:
                        yield sse_event("dataset", {"message": f"ナレッジ登録受付: doc_id={doc_id}"})
                    if batch:
                        yield sse_event("dataset", {"message": f"埋め込み進捗監視開始: batch={batch}"})

                    if batch:
                        # インデックス完了までポーリング
                        poll_count = 0
                        while True:
                            poll_count += 1
                            st = dify_get_indexing_status(
                                base=base_ds,
                                key=key_ds,
                                dataset_id=dataset_id,
                                batch=batch,
                            )
                            msg, done, level = summarize_indexing_status(st)
                            yield sse_event("dataset", {"message": msg, "level": level})
                            if done:
                                break
                            if poll_count >= 120:
                                yield sse_event("dataset", {"message": "進捗監視タイムアウト（120回）。処理はDify側で継続中の可能性があります。", "level": "bad"})
                                break
                            time.sleep(2.0)

                    ok_count += 1

                except Exception as e:
                    ng_count += 1
                    yield sse_event("error_one", {"file": relpath, "error": safe_err(str(e))})

            yield sse_event("summary", {
                "ok": ok_count,
                "ng": ng_count,
                "skip": skip_count,
                "total": len(files),
                "overwrite": overwrite,
            })

        return Response(sse(), mimetype="text/event-stream")

    return app


# -----------------------------
# File scan
# -----------------------------

def list_files(root_dir: str, recursive: bool = True) -> List[str]:
    results: List[str] = []
    root_dir = os.path.abspath(root_dir)

    if recursive:
        for base, _, files in os.walk(root_dir):
            for name in files:
                ext = os.path.splitext(name)[1].lower()
                if ext in ALLOWED_EXTS:
                    abs_path = os.path.join(base, name)
                    rel = os.path.relpath(abs_path, root_dir)
                    results.append(rel)
    else:
        for name in os.listdir(root_dir):
            abs_path = os.path.join(root_dir, name)
            if os.path.isfile(abs_path):
                ext = os.path.splitext(name)[1].lower()
                if ext in ALLOWED_EXTS:
                    results.append(name)

    results.sort()
    return results


# -----------------------------
# Extract
# -----------------------------

def extract_text(path: str, knowledge_style: str = "rag_markdown") -> Tuple[str, Dict[str, str]]:
    ext = os.path.splitext(path)[1].lower()
    stat = os.stat(path)
    meta = {
        "filename": os.path.basename(path),
        "ext": ext,
        "size_bytes": str(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    }

    if ext in {
        ".txt", ".md", ".csv", ".json", ".log",
        ".html", ".xml", ".yml", ".yaml", ".ini", ".conf",
        ".py", ".js", ".css",
    }:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read(), meta

    if ext == ".docx":
        doc = Document(path)
        parts = []
        for p in doc.paragraphs:
            t = p.text.strip()
            if t:
                parts.append(t)
        return "\n".join(parts), meta

    if ext == ".pdf":
        return extract_pdf_like(path), meta

    if ext in {".xlsx", ".xlsm", ".xls"}:
        if knowledge_style == "rag_natural":
            text = extract_excel_as_markdown_tables(path, ext)
        else:
            text = extract_excel_as_row_records(path, ext)
        return text, meta

    if ext in {".ppt", ".pptx"}:
        return extract_ppt_like(path, ext), meta

    raise RuntimeError(f"未対応の拡張子です: {ext}")


def extract_pdf_like(path: str) -> str:
    reader = PdfReader(path)
    parts = []
    for i, page in enumerate(reader.pages):
        txt = page.extract_text() or ""
        txt = normalize_pdf_like_text(txt)
        if txt.strip():
            parts.append(f"[PAGE {i+1}]\n{txt}")
    return "\n\n".join(parts)


def extract_excel_as_row_records(path: str, ext: str) -> str:
    if ext == ".xls":
        return extract_xls_as_row_records(path)
    return extract_xlsx_like_as_row_records(path)


def extract_xlsx_like_as_row_records(path: str) -> str:
    wb = load_workbook(path, data_only=True, read_only=True)
    out: List[str] = []

    for sheet in wb.worksheets:
        out.append(f"[SHEET: {sheet.title}]")

        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            out.append("[EMPTY]")
            out.append("")
            continue

        header: Optional[List[str]] = None
        start_idx = 0
        for i, r in enumerate(rows):
            if any(cell is not None and str(cell).strip() != "" for cell in r):
                header = [sanitize_header(cell) for cell in r]
                start_idx = i + 1
                break

        if not header:
            out.append("[EMPTY]")
            out.append("")
            continue

        out.append("[HEADER] " + "\t".join([h if h else "" for h in header]))

        for ridx in range(start_idx, len(rows)):
            r = rows[ridx]
            if not any(cell is not None and str(cell).strip() != "" for cell in r):
                continue

            record = {}
            for cidx, cell in enumerate(r):
                key = header[cidx] if cidx < len(header) else f"COL{cidx+1}"
                if not key:
                    key = f"COL{cidx+1}"
                val = "" if cell is None else str(cell).strip()
                if val != "":
                    record[key] = val

            if record:
                out.append("[ROW] " + json.dumps(record, ensure_ascii=False, separators=(",", ":")))

        out.append("")

    return "\n".join(out).strip()


def extract_xls_as_row_records(path: str) -> str:
    wb = xlrd.open_workbook(path)
    out: List[str] = []

    for sheet in wb.sheets():
        out.append(f"[SHEET: {sheet.name}]")

        if sheet.nrows <= 0:
            out.append("[EMPTY]")
            out.append("")
            continue

        rows = []
        for r in range(sheet.nrows):
            rows.append([sheet.cell_value(r, c) for c in range(sheet.ncols)])

        header: Optional[List[str]] = None
        start_idx = 0
        for i, r in enumerate(rows):
            if any(cell is not None and str(cell).strip() != "" for cell in r):
                header = [sanitize_header(cell) for cell in r]
                start_idx = i + 1
                break

        if not header:
            out.append("[EMPTY]")
            out.append("")
            continue

        out.append("[HEADER] " + "\t".join([h if h else "" for h in header]))

        for ridx in range(start_idx, len(rows)):
            r = rows[ridx]
            if not any(cell is not None and str(cell).strip() != "" for cell in r):
                continue

            record = {}
            for cidx, cell in enumerate(r):
                key = header[cidx] if cidx < len(header) else f"COL{cidx+1}"
                if not key:
                    key = f"COL{cidx+1}"
                val = "" if cell is None else str(cell).strip()
                if val != "":
                    record[key] = val

            if record:
                out.append("[ROW] " + json.dumps(record, ensure_ascii=False, separators=(",", ":")))

        out.append("")

    return "\n".join(out).strip()


def extract_excel_as_markdown_tables(path: str, ext: str) -> str:
    if ext == ".xls":
        return extract_xls_as_markdown_tables(path)
    return extract_xlsx_like_as_markdown_tables(path)


def extract_xlsx_like_as_markdown_tables(path: str) -> str:
    MAX_ROWS_PER_SHEET = 200
    wb = load_workbook(path, data_only=True, read_only=True)

    out: List[str] = []
    for sheet in wb.worksheets:
        out.append(f"[SHEET: {sheet.title}]")

        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            out.append("(empty)")
            out.append("")
            continue

        header = None
        start_idx = 0
        for i, r in enumerate(rows):
            if any(cell is not None and str(cell).strip() != "" for cell in r):
                header = [sanitize_header(c) for c in r]
                start_idx = i + 1
                break
        if not header:
            out.append("(empty)")
            out.append("")
            continue

        cols = [h if h else f"COL{j+1}" for j, h in enumerate(header)]

        out.append("| " + " | ".join(cols) + " |")
        out.append("| " + " | ".join(["---"] * len(cols)) + " |")

        count = 0
        for ridx in range(start_idx, len(rows)):
            r = rows[ridx]
            if not any(cell is not None and str(cell).strip() != "" for cell in r):
                continue

            vals = []
            for cidx in range(len(cols)):
                cell = r[cidx] if cidx < len(r) else None
                v = "" if cell is None else str(cell).strip()
                v = v.replace("\n", " ").replace("\r", " ")
                v = v.replace("|", "\\|")
                vals.append(v)

            out.append("| " + " | ".join(vals) + " |")
            count += 1
            if count >= MAX_ROWS_PER_SHEET:
                out.append(f"(… {MAX_ROWS_PER_SHEET}行まで表示。続きは省略 …)")
                break

        out.append("")

    return "\n".join(out).strip()


def extract_xls_as_markdown_tables(path: str) -> str:
    MAX_ROWS_PER_SHEET = 200
    wb = xlrd.open_workbook(path)
    out: List[str] = []

    for sheet in wb.sheets():
        out.append(f"[SHEET: {sheet.name}]")

        if sheet.nrows <= 0:
            out.append("(empty)")
            out.append("")
            continue

        rows = []
        for r in range(sheet.nrows):
            rows.append([sheet.cell_value(r, c) for c in range(sheet.ncols)])

        header = None
        start_idx = 0
        for i, r in enumerate(rows):
            if any(cell is not None and str(cell).strip() != "" for cell in r):
                header = [sanitize_header(c) for c in r]
                start_idx = i + 1
                break
        if not header:
            out.append("(empty)")
            out.append("")
            continue

        cols = [h if h else f"COL{j+1}" for j, h in enumerate(header)]

        out.append("| " + " | ".join(cols) + " |")
        out.append("| " + " | ".join(["---"] * len(cols)) + " |")

        count = 0
        for ridx in range(start_idx, len(rows)):
            r = rows[ridx]
            if not any(cell is not None and str(cell).strip() != "" for cell in r):
                continue

            vals = []
            for cidx in range(len(cols)):
                cell = r[cidx] if cidx < len(r) else None
                v = "" if cell is None else str(cell).strip()
                v = v.replace("\n", " ").replace("\r", " ")
                v = v.replace("|", "\\|")
                vals.append(v)

            out.append("| " + " | ".join(vals) + " |")
            count += 1
            if count >= MAX_ROWS_PER_SHEET:
                out.append(f"(… {MAX_ROWS_PER_SHEET}行まで表示。続きは省略 …)")
                break

        out.append("")

    return "\n".join(out).strip()


def extract_ppt_like(path: str, ext: str) -> str:
    try:
        prs = Presentation(path)
    except Exception:
        if ext == ".ppt":
            raise RuntimeError("`.ppt`（旧形式）は python-pptx で直接読めない場合があります。`.pptx` に変換して再実行してください。")
        raise RuntimeError("PowerPointの解析に失敗しました。ファイル破損または形式が想定外です。")

    parts: List[str] = []
    for i, slide in enumerate(prs.slides):
        slide_text: List[str] = []
        for shape in slide.shapes:
            try:
                if not hasattr(shape, "text"):
                    continue
                t = (shape.text or "").strip()
                if t:
                    slide_text.append(t)
            except Exception:
                continue

        if slide_text:
            parts.append(f"[SLIDE {i+1}]\n" + "\n".join(slide_text))

    return "\n\n".join(parts).strip()


def sanitize_header(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_pdf_like_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = text.replace("\r", "\n")

    lines = [ln.rstrip() for ln in text.split("\n")]
    out: List[str] = []
    buf = ""

    def flush():
        nonlocal buf
        if buf.strip():
            out.append(buf.strip())
        buf = ""

    for ln in lines:
        t = ln.strip()
        if not t:
            flush()
            continue

        if not buf:
            buf = t
            continue

        if re.search(r"[\u3002\.!?]$", buf):
            flush()
            buf = t
            continue

        if re.search(r"[-–—]$", buf):
            buf = buf[:-1] + t
            continue

        if len(buf) < 80 and len(t) < 80:
            buf += " " + t
        else:
            flush()
            out.append(t)
    flush()

    text2 = "\n".join(out)
    text2 = re.sub(r"\n{3,}", "\n\n", text2)
    return text2.strip()


# -----------------------------
# Output
# -----------------------------

def make_output_path(output_dir: str, rel_input_path: str) -> str:
    base, _ = os.path.splitext(rel_input_path)
    safe = sanitize_relpath(base) + ".md"
    return os.path.join(output_dir, safe)


def sanitize_relpath(p: str) -> str:
    p = p.replace("..", "__")
    p = re.sub(r'[<>:"|?*]', "_", p)
    return p


def wrap_markdown_with_source_meta(answer_md: str, meta: Dict[str, str]) -> str:
    # ② 生成Markdownに「変換前の元ファイルのフルパス/メタデータ」を付与
    # YAML front matter 形式（RAGでも扱いやすい）
    relpath = meta.get("relpath", "")
    abspath = meta.get("abspath", "")
    filename = meta.get("filename", "")
    ext = meta.get("ext", "")
    size_bytes = meta.get("size_bytes", "")
    mtime = meta.get("mtime", "")

    fm = (
        "---\n"
        f"source_relpath: {relpath}\n"
        f"source_abspath: {abspath}\n"
        f"source_filename: {filename}\n"
        f"source_ext: {ext}\n"
        f"source_size_bytes: {size_bytes}\n"
        f"source_mtime: {mtime}\n"
        f"generated_at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "---\n\n"
    )

    return fm + (answer_md.strip() + "\n")


# -----------------------------
# SSE / safety
# -----------------------------

def sse_event(event: str, data: Dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def safe_err(msg: str) -> str:
    if not msg:
        return "不明なエラー"
    msg = re.sub(r"(app-[A-Za-z0-9_\-]{10,})", "app-***REDACTED***", msg)
    msg = re.sub(r"(Bearer\s+)[A-Za-z0-9_\-\.=]+", r"\1***REDACTED***", msg, flags=re.IGNORECASE)
    msg = re.sub(r"https?://[^\s]+", "[URL_REDACTED]", msg)
    return msg[:400]


# -----------------------------
# Dify Chat
# -----------------------------

def convert_via_dify_chat_messages_secure(
    api_base: str,
    api_key: str,
    user: str,
    source_path: str,
    source_meta: Dict[str, str],
    text: str,
    knowledge_style: str,
    chunk_sep: str,
) -> str:
    url = f"{api_base}/chat-messages"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    instruction = build_rag_instruction(
        source_path=source_path,
        source_meta=source_meta,
        knowledge_style=knowledge_style,
        chunk_sep=chunk_sep,
    )

    query = (
        instruction
        + "\n\n===== SOURCE TEXT BEGIN =====\n"
        + text
        + "\n===== SOURCE TEXT END =====\n"
    )

    payload = {
        "inputs": {},
        "query": query,
        "response_mode": "blocking",
        "user": user,
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=REQ_TIMEOUT_SEC)
    except requests.RequestException:
        raise RuntimeError("API通信に失敗しました（ネットワーク/タイムアウト）。")

    if r.status_code >= 400:
        raise RuntimeError(f"APIエラー（HTTP {r.status_code}）。")

    try:
        data = r.json()
    except Exception:
        raise RuntimeError("APIレスポンスの解析に失敗しました。")

    answer = data.get("answer")
    if not answer or not isinstance(answer, str):
        raise RuntimeError("APIレスポンスが想定外です（answerがありません）。")

    return answer.strip() + "\n"


def build_rag_instruction(source_path: str, source_meta: Dict[str, str], knowledge_style: str, chunk_sep: str) -> str:
    meta_lines = "\n".join([f"- {k}: {v}" for k, v in source_meta.items()])
    ext = (source_meta.get("ext") or "").lower()

    first_chunk_rule = f"""
        # 最初のチャンク（必須）
        - 出力の最初のチャンクは必ず「全体構成（目次/分類）」にする。
        - 形式例：
        - 見出し: 「## 全体構成（目次/分類）」
        - 次の1文: 「このチャンクでは文書全体の構成（目次）と分類方針を示す。」
        - 続けて、章立て（大カテゴリ）と、その中で扱う内容の要約を箇条書きで書く。
        - そのチャンクの末尾に必ず「{chunk_sep}」を単独行で置く。
        """

    excel_rules = ""
    if ext in {".xlsx", ".xls", ".xlsm"} and knowledge_style != "rag_natural":
        excel_rules = f"""
        # Excel特別ルール（標準/FAQ用）
        - 入力には [HEADER] と [ROW] が含まれる。
        - 出力は「データ行（[ROW]）1つにつき、必ずチャンク1つ」にする。
        - チャンク区切りは必ず「{chunk_sep}」の単独行にする。
        - [ROW]を統合しない。行同士をまとめない。
        """

    if knowledge_style == "rag_natural":
        style_block = f"""
        出力はMarkdownで「RAG向けMarkdown（自然言語）」として整形する。

        # 手順（必須）
        1) まず文書全体の構成を把握し、上位の章立て（大カテゴリ）を作る。
        2) 次に、人間が指示を出すような自然文で各チャンクの目的を宣言してから本文を置く。
        3) チャンク区切りは必ず「{chunk_sep}」の単独行にする。

        # チャンクの書き方（必須）
        - 各チャンクは次の順序で書く：
        - 見出し（例：## ○○）
        - 「このチャンクでは〜を説明する。」のような自然言語の導入文
        - 本文（要点→詳細→手順→例→注意）
        - 見出しは検索されやすいキーワードを含める（固有名詞/手順名/条件/例外/閾値）。

        # 分割方針（必須）
        - 「章/節/話題/手順のまとまり」で区切る。
        - 長い場合は、手順や観点で追加分割してよい。
        - ただし情報は省略しない（重複は統合可）。
        """
    elif knowledge_style == "faq":
        style_block = f"""
        出力はMarkdownで、FAQ形式にする。
        - 質問は具体的に、回答は短く「結論→根拠→例」の順にする。
        - チャンク区切りは必ず「{chunk_sep}」の単独行にする。
        """
    else:
        style_block = f"""
        出力はMarkdownで、RAGに最適化したナレッジへ整形する。
        - 文は「主語 + 述語」でできるだけ明確にする。
        - 検索されやすいキーワード（固有名詞/手順名/条件/例外/閾値）を含める。
        - チャンク区切りは必ず「{chunk_sep}」の単独行にする。
        - 情報を省略しない（重複は統合可）。
        """

    return f"""
        あなたは「社内RAG用ナレッジ整形AI」である。
        入力された文章を、検索精度が最大化するMarkdownへ再構成する。

        # 変換対象ファイル
        - path: {source_path}
        - meta:
        {meta_lines}

        # 絶対ルール
        - 出力は「変換後Markdown本文のみ」とする（前置き/解説/謝罪/注釈は禁止）。
        - 原文が曖昧な場合は「〜である可能性がある」等で補い、捏造しない。
        - チャンク区切りは必ず「{chunk_sep}」の単独行にする。

        {first_chunk_rule}

        {excel_rules}

        # スタイル
        {style_block}
        """.strip()


# -----------------------------
# Dify Dataset API
# -----------------------------

def get_dataset_api_config() -> Tuple[str, str]:
    # 専用設定があれば優先、なければ /chat-messages の設定を流用
    base = DATASET_API_BASE or API_BASE
    key = DATASET_API_KEY or API_KEY
    return base, key


def dify_list_datasets(base: str, key: str) -> List[Dict]:
    # Dify Docs: GET /datasets
    url = f"{base}/datasets"
    headers = {"Authorization": f"Bearer {key}"}
    params = {"page": 1, "limit": 100}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"ナレッジ一覧取得に失敗しました（HTTP {r.status_code}）。")

    data = r.json()
    # 返り値は {data:[...], has_more, limit, page, total}
    return data.get("data") or []


def dify_get_dataset_name(base: str, key: str, dataset_id: str) -> Optional[str]:
    # 一覧から引く（APIに直接のgetが無い環境もあるため）
    items = dify_list_datasets(base=base, key=key)
    for it in items:
        if (it.get("id") or "") == dataset_id:
            return it.get("name")
    return None


def dify_create_doc_by_text(
    base: str,
    key: str,
    dataset_id: str,
    name: str,
    text: str,
    separator: str,
    max_tokens: int,
    doc_form: str,
) -> Dict:
    # Dify Docs: POST /datasets/{dataset_id}/document/create-by-text
    url = f"{base}/datasets/{dataset_id}/document/create-by-text"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    payload = {
        "name": name,
        "text": text,
        "indexing_technique": "high_quality",
        "doc_form": doc_form,
        "doc_language": "Japanese",
        "process_rule": {
            "mode": "automatic",
            "rules": {
                "pre_processing_rules": [
                    {"id": "remove_extra_spaces", "enabled": True},
                ],
                "segmentation": {
                    "separator": separator,
                    "max_tokens": max_tokens,
                },
                "parent_mode": "full-doc",
                "subchunk_segmentation": {
                    "separator": separator,
                    "max_tokens": max(200, int(max_tokens * 0.6)),
                    "chunk_overlap": 50,
                },
            },
        },
        "retrieval_model": {
            "search_method": "hybrid_search",
            "reranking_enable": False,
            "top_k": 8,
            "score_threshold_enabled": False,
        },
    }

    r = requests.post(url, headers=headers, json=payload, timeout=REQ_TIMEOUT_SEC)
    if r.status_code >= 400:
        raise RuntimeError(f"ナレッジ登録に失敗しました（HTTP {r.status_code}）。")

    return r.json()


def dify_get_indexing_status(base: str, key: str, dataset_id: str, batch: str) -> Dict:
    # Dify Docs: GET /datasets/{dataset_id}/documents/{batch}/indexing-status
    url = f"{base}/datasets/{dataset_id}/documents/{batch}/indexing-status"
    headers = {"Authorization": f"Bearer {key}"}

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"進捗取得に失敗しました（HTTP {r.status_code}）。")

    return r.json()


def summarize_indexing_status(st: Dict) -> Tuple[str, bool, str]:
    # data: [{indexing_status, completed_segments, total_segments, error, ...}]
    rows = st.get("data") or []
    if not rows:
        return ("埋め込み進捗: (no data)", False, "info")

    row = rows[0]
    status = (row.get("indexing_status") or "").lower()
    done_seg = row.get("completed_segments")
    total_seg = row.get("total_segments")
    err = row.get("error")

    if err:
        return (f"埋め込みエラー: {safe_err(str(err))}", True, "bad")

    if isinstance(done_seg, int) and isinstance(total_seg, int) and total_seg > 0:
        msg = f"埋め込み進捗: {done_seg}/{total_seg} (status={status})"
    else:
        msg = f"埋め込み進捗: status={status}"

    if status in {"completed", "succeeded", "success"}:
        return (msg + " → 完了", True, "ok")

    if status in {"failed", "error", "stopped"}:
        return (msg + " → 失敗", True, "bad")

    return (msg, False, "info")


# -----------------------------
# Heuristics
# -----------------------------

def estimate_max_tokens(md: str) -> int:
    # 文字数からざっくり推定（Difyのmax_tokensはトークンなので近似）
    n = len(md)
    if n < 8_000:
        return 400
    if n < 25_000:
        return 700
    if n < 60_000:
        return 1000
    return 1400


def looks_hierarchical(md: str) -> bool:
    # 見出しが多い&チャンク区切りが複数あるなら hierarchical を優先
    h = len(re.findall(r"^#{2,3}\s+", md, flags=re.MULTILINE))
    return h >= 12


# -----------------------------
# main
# -----------------------------

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5211, debug=False, threaded=True)
