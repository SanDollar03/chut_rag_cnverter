(() => {
  const $ = (id) => document.getElementById(id);

  const inputDir = $("inputDir");
  const outputDir = $("outputDir");
  const recursive = $("recursive");
  const overwrite = $("overwrite");

  const user = $("user");
  const style = $("style");
  const chunkSep = $("chunkSep");

  const datasetSel = $("dataset");
  const reloadDatasetsBtn = $("reloadDatasetsBtn");

  const scanBtn = $("scanBtn");
  const runBtn = $("runBtn");

  const fileList = $("fileList");
  const log = $("log");

  const noticeModal = $("noticeModal");
  const noticeBody = $("noticeBody");
  const noticeClose = $("noticeClose");
  const noticeOk = $("noticeOk");

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function appendLogHtml(html) {
    log.innerHTML += html;
    log.scrollTop = log.scrollHeight;
  }

  function addLog(line, kind = "info") {
    const ts = new Date().toLocaleTimeString();
    const text = escapeHtml(line);
    let tagClass = "info";
    let tagLabel = "[..]";
    if (kind === "ok") {
      tagClass = "ok";
      tagLabel = "[OK]";
    } else if (kind === "bad") {
      tagClass = "err";
      tagLabel = "[ERR]";
    } else if (kind === "skip") {
      tagClass = "skip";
      tagLabel = "[SKIP]";
    }
    const html = `<div class="log-line"><span class="log-ts">${ts}</span> <span class="log-tag ${tagClass}">${tagLabel}</span> <span class="log-msg">${text}</span></div>`;
    appendLogHtml(html);
  }

  function addTwoLine(filePath, secondLine, kind = "ok") {
    const ts = new Date().toLocaleTimeString();
    const fileEsc = escapeHtml(filePath);
    const secondEsc = escapeHtml(secondLine);
    let tagClass = "ok";
    let tagLabel = "[OK]";
    if (kind === "err") {
      tagClass = "err";
      tagLabel = "[ERR]";
    } else if (kind === "skip") {
      tagClass = "skip";
      tagLabel = "[SKIP]";
    }
    const first = `<div class="log-line"><span class="log-ts">${ts}</span> <span class="log-tag ${tagClass}">${tagLabel}</span> <span class="log-msg">保存: ${fileEsc}</span></div>`;
    const second = `<div class="log-line log-sub"><span class="log-submark">&gt;</span> <span class="log-submsg">${secondEsc}</span></div>`;
    appendLogHtml(first + second);
  }

  function addErrorTwoLine(filePath, errMsg) {
    const ts = new Date().toLocaleTimeString();
    const fileEsc = escapeHtml(filePath);
    const errEsc = escapeHtml(errMsg);
    const first = `<div class="log-line"><span class="log-ts">${ts}</span> <span class="log-tag err">[ERR]</span> <span class="log-msg">失敗: ${fileEsc}</span></div>`;
    const second = `<div class="log-line log-sub"><span class="log-submark">&gt;</span> <span class="log-submsg">${errEsc}</span></div>`;
    appendLogHtml(first + second);
  }

  function setFiles(files) {
    fileList.textContent = files && files.length ? files.map((f) => `- ${f}`).join("\n") : "(対象なし)";
  }

  async function getJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  }

  async function postJSON(url, body) {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  }

  function openNoticeModal(text) {
    if (!noticeModal || !noticeBody) return;
    noticeBody.textContent = text ?? "";
    noticeModal.setAttribute("aria-hidden", "false");
    noticeModal.classList.add("show");
    document.body.classList.add("modalOpen");
  }

  function closeNoticeModal() {
    if (!noticeModal) return;
    noticeModal.setAttribute("aria-hidden", "true");
    noticeModal.classList.remove("show");
    document.body.classList.remove("modalOpen");
  }

  function navType() {
    try {
      const nav = performance.getEntriesByType?.("navigation");
      if (nav && nav.length) return nav[0].type || null;
    } catch { }
    return null;
  }

  function shouldShowNoticeOnThisLoad() {
    const t = navType();
    if (t === "reload") return true;
    if (t === "navigate") return false;

    const KEY = "notice_seen_first_load_auto";
    try {
      const seen = sessionStorage.getItem(KEY);
      if (!seen) {
        sessionStorage.setItem(KEY, "1");
        return false;
      }
      return true;
    } catch {
      return false;
    }
  }

  async function showNoticeIfNeeded() {
    if (!shouldShowNoticeOnThisLoad()) return;
    try {
      const data = await getJSON("/api/notice");
      openNoticeModal(data.text || "");
    } catch (e) {
      addLog(`NOTICE取得失敗: ${e.message}`, "bad");
    }
  }

  function rebuildDatasetSelect(items, prefix) {
    const all = Array.isArray(items) ? items : [];
    datasetSel.innerHTML = "";

    if (!all.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = `（${prefix || ""} で始まるナレッジがありません）`;
      datasetSel.appendChild(opt);
      datasetSel.disabled = true;
      return;
    }

    for (const it of all) {
      const id = it?.id || "";
      const name = it?.name || "";
      if (!id || !name) continue;
      const opt = document.createElement("option");
      opt.value = id;
      opt.textContent = name;
      datasetSel.appendChild(opt);
    }

    datasetSel.disabled = false;
  }

  async function loadDatasets() {
    try {
      addLog("ナレッジ一覧取得中...");
      const data = await getJSON("/api/datasets");
      rebuildDatasetSelect(data.items, data.prefix);
      addLog(`ナレッジ: ${data.prefix || ""}* のみ → ${data.items?.length || 0}件`, "ok");
    } catch (e) {
      datasetSel.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "(ナレッジ一覧の取得に失敗)";
      datasetSel.appendChild(opt);
      datasetSel.disabled = true;
      addLog(`ナレッジ取得失敗: ${e.message}`, "bad");
    }
  }

  noticeClose?.addEventListener("click", closeNoticeModal);
  noticeOk?.addEventListener("click", closeNoticeModal);
  noticeModal?.addEventListener("click", (e) => {
    if (e.target === noticeModal) closeNoticeModal();
  });
  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && noticeModal?.classList.contains("show")) {
      closeNoticeModal();
    }
  });

  reloadDatasetsBtn?.addEventListener("click", loadDatasets);

  scanBtn.addEventListener("click", async () => {
    log.innerHTML = "";
    fileList.textContent = "";
    addLog("スキャン開始");

    try {
      const data = await postJSON("/api/scan", {
        input_dir: inputDir.value.trim(),
        recursive: recursive.checked,
      });
      setFiles(data.files);
      addLog(`対象: ${data.count} 件`, "ok");
    } catch (e) {
      addLog(e.message, "bad");
    }
  });

  runBtn.addEventListener("click", async () => {
    log.innerHTML = "";
    addLog("変換 → ナレッジ登録開始（SSE）");
    runBtn.disabled = true;
    scanBtn.disabled = true;
    reloadDatasetsBtn && (reloadDatasetsBtn.disabled = true);

    try {
      const datasetId = datasetSel?.value || "";
      if (!datasetId) throw new Error("ナレッジを選択してください。");

      const payload = {
        input_dir: inputDir.value.trim(),
        output_dir: outputDir.value.trim(),
        recursive: recursive.checked,
        overwrite: !!overwrite?.checked,

        dataset_id: datasetId,
        user: (user?.value || "rag_converter").trim() || "rag_converter",
        knowledge_style: style?.value || "rag_markdown",
        chunk_sep: (chunkSep?.value || "***").trim() || "***",
      };

      const r = await fetch("/api/auto/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const data = await r.json().catch(() => ({}));
        throw new Error(data.error || `HTTP ${r.status}`);
      }

      const reader = r.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buf = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buf += decoder.decode(value, { stream: true });

        let idx;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);

          let ev = "message";
          let dataLine = null;
          for (const line of raw.split("\n")) {
            if (line.startsWith("event:")) ev = line.slice(6).trim();
            if (line.startsWith("data:")) dataLine = line.slice(5).trim();
          }
          if (!dataLine) continue;

          let obj = null;
          try {
            obj = JSON.parse(dataLine);
          } catch {
            obj = { raw: dataLine };
          }

          if (ev === "meta") {
            const ow = obj.overwrite ? "ON" : "OFF";
            addLog(`総件数: ${obj.total} / 上書き: ${ow} / dataset_id: ${obj.dataset_id}`);
            addLog(`chunk_sep: ${obj.chunk_sep}`);
          } else if (ev === "progress") {
            addLog(`(${obj.index}/${obj.total}) ${obj.file}`);
          } else if (ev === "skip_one") {
            addTwoLine(obj.file, obj.out || "", "skip");
          } else if (ev === "done_one") {
            addTwoLine(obj.file, obj.out || "", "ok");
          } else if (ev === "error_one") {
            addErrorTwoLine(obj.file, obj.error || "不明なエラー");
          } else if (ev === "dataset") {
            const patch = obj.dataset_patch_ok === false ? ` dataset_patch=ERR${obj.dataset_patch_error ? ' (' + obj.dataset_patch_error + ')' : ''}` : ' dataset_patch=OK';
            const msg =
              `ナレッジ登録: doc_id=${obj.doc_id} batch=${obj.batch} ` +
              `chunks=${obj.chunks} max_chunk_tokens~=${obj.chunk_tokens_max} ` +
              `set max_tokens=${obj.dify_max_tokens} sep=${obj.chunk_sep} search=${obj.search_method}` +
              patch;
            addLog(msg);
          } else if (ev === "dataset_progress") {
            const st = String(obj.status || "");
            const stLc = st.toLowerCase();
            const cs = Number(obj.completed_segments || 0);
            const ts = Number(obj.total_segments || 0);
            const seg = `${cs}/${ts}`;
            const term = obj.terminal ? "(terminal)" : "";
            const line = `埋め込み進捗: status=${st} segments=${seg} ${term}`;

            if (stLc === "completed" && ts > 0) {
              addLog(line + " → 完了", "ok");
            } else if (stLc === "completed" && ts === 0) {
              addLog(line + " → チャンク0（本文空/区切り不一致の可能性）", "bad");
            } else if (stLc === "error") {
              addLog(line + ` error=${obj.error || ""}`.trim(), "bad");
            } else {
              addLog(line);
            }
          } else if (ev === "dataset_done") {
            const st = String(obj.status || "");
            const stLc = st.toLowerCase();
            const cs = Number(obj.completed_segments || 0);
            const ts = Number(obj.total_segments || 0);
            const seg = `${cs}/${ts}`;
            const base = `ナレッジ登録完了: status=${st} segments=${seg}`;
            if (stLc === "completed" && ts > 0 && cs >= ts) {
              addLog(base, "ok");
            } else {
              addLog(base + (obj.error ? ` error=${obj.error}` : ""), "bad");
            }
          } else if (ev === "summary") {
            addLog(`完了: OK=${obj.ok}, SKIP=${obj.skip}, NG=${obj.ng}, TOTAL=${obj.total}`, "ok");
          }
        }
      }

      addLog("SSE終了");
    } catch (e) {
      addLog(e.message, "bad");
    } finally {
      runBtn.disabled = false;
      scanBtn.disabled = false;
      reloadDatasetsBtn && (reloadDatasetsBtn.disabled = false);
    }
  });

  loadDatasets();
  showNoticeIfNeeded();
})();
