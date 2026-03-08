(() => {
  const $ = (id) => document.getElementById(id);

  const datasetSel = $("dataset");
  const reloadDatasetsBtn = $("reloadDatasetsBtn");
  const docKeyword = $("docKeyword");
  const refreshBtn = $("refreshBtn");
  const clearBtn = $("clearBtn");

  const docsSummary = $("docsSummary");
  const docsHint = $("docsHint");
  const docsTbody = $("docsTbody");

  const segModal = $("segModal");
  const segTitle = $("segTitle");
  const segClose = $("segClose");
  const segOk = $("segOk");
  const segKeyword = $("segKeyword");
  const segStatus = $("segStatus");
  const segSearchBtn = $("segSearchBtn");
  const segPrevBtn = $("segPrevBtn");
  const segNextBtn = $("segNextBtn");
  const segPageInfo = $("segPageInfo");
  const segTotalInfo = $("segTotalInfo");
  const segTbody = $("segTbody");
  const segDetail = $("segDetail");

  const noticeModal = $("noticeModal");
  const noticeBody = $("noticeBody");
  const noticeClose = $("noticeClose");
  const noticeOk = $("noticeOk");

  const state = {
    loadToken: 0,
    datasetId: "",
    docs: [],
    seg: {
      open: false,
      datasetId: "",
      docId: "",
      docName: "",
      page: 1,
      limit: 20,
      hasMore: false,
      total: 0,
    },
  };

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  async function getJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  }

  function openNotice(text) {
    if (!noticeModal || !noticeBody) return;
    noticeBody.textContent = text || "";
    noticeModal.setAttribute("aria-hidden", "false");
    noticeModal.classList.add("show");
    document.body.classList.add("modalOpen");
  }

  function closeNotice() {
    if (!noticeModal) return;
    noticeModal.setAttribute("aria-hidden", "true");
    noticeModal.classList.remove("show");
    document.body.classList.remove("modalOpen");
  }

  async function bootNoticeModal() {
    if (!noticeModal || !noticeBody) return;
    try {
      const d = await getJSON("/api/notice");
      const txt = (d.text || "").trim();
      if (txt) openNotice(txt);
    } catch {
      // ignore
    }

    noticeClose?.addEventListener("click", closeNotice);
    noticeOk?.addEventListener("click", closeNotice);
    noticeModal?.addEventListener("click", (e) => {
      if (e.target === noticeModal) closeNotice();
    });
  }

  function toDateString(ts) {
    if (!ts) return "-";
    const n = Number(ts);
    if (!Number.isFinite(n)) return "-";
    // Difyが秒/ミリ秒どちらでも来る可能性がある
    return new Date(n > 1e12 ? n : n * 1000).toLocaleString();
  }

  function statusKind(s) {
    const v = String(s || "").toLowerCase();
    if (!v) return "";
    if (v === "completed" || v === "success") return "ok";
    if (v === "error" || v === "failed" || v === "stopped") return "err";
    if (
      v === "indexing" ||
      v === "waiting" ||
      v === "parsing" ||
      v === "splitting" ||
      v === "cleaning" ||
      v === "queuing"
    )
      return "warn";
    return "";
  }

  function pill(text, kind = "") {
    const k = kind ? ` ${kind}` : "";
    return `<span class="pillSmall${k}">${escapeHtml(text || "-")}</span>`;
  }

  function setDocsHint(text) {
    if (docsHint) docsHint.textContent = text || "-";
  }

  function setDocsSummary(text) {
    if (docsSummary) docsSummary.textContent = text || "-";
  }

  function renderDocs(docs) {
    if (!docsTbody) return;
    docsTbody.innerHTML = "";

    const list = Array.isArray(docs) ? docs : [];
    if (!list.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="5" class="kEmpty">(ドキュメントなし)</td>`;
      docsTbody.appendChild(tr);
      return;
    }

    for (const d of list) {
      const id = d?.id || "";
      const name = d?.name || "";
      const wc = d?.word_count ?? "-";
      const hc = d?.hit_count ?? "-";
      const createdAt = toDateString(d?.created_at);
      const st = d?.indexing_status || d?.display_status || "-";

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="kName">
          <a href="#" class="docLink" data-docid="${escapeHtml(id)}" data-docname="${escapeHtml(name)}">${escapeHtml(
        name || id
      )}</a>
        </td>
        <td class="kNum">${escapeHtml(String(wc))}</td>
        <td class="kNum">${escapeHtml(String(hc))}</td>
        <td class="kTime">${escapeHtml(createdAt)}</td>
        <td class="kStatus">${pill(st, statusKind(st))}</td>
      `;
      docsTbody.appendChild(tr);
    }

    docsTbody.querySelectorAll("a.docLink").forEach((a) => {
      a.addEventListener("click", (e) => {
        e.preventDefault();
        const docId = a.getAttribute("data-docid") || "";
        const docName = a.getAttribute("data-docname") || docId;
        if (!state.datasetId || !docId) return;
        openSegModal(state.datasetId, docId, docName);
      });
    });
  }

  async function loadDatasets() {
    const d = await getJSON("/api/datasets");
    const items = d.items || [];

    if (!datasetSel) return;
    datasetSel.innerHTML = "";

    for (const it of items) {
      const opt = document.createElement("option");
      opt.value = it.id;
      opt.textContent = it.name;
      datasetSel.appendChild(opt);
    }

    state.datasetId = items.length ? datasetSel.value : "";
  }

  function keywordFilter(list, kw) {
    const k = String(kw || "").trim().toLowerCase();
    if (!k) return list;
    return (list || []).filter((d) => String(d?.name || "").toLowerCase().includes(k));
  }

  async function loadDocuments() {
    const token = ++state.loadToken;

    if (!state.datasetId) {
      state.docs = [];
      setDocsSummary("-");
      setDocsHint("dataset未選択");
      renderDocs([]);
      return;
    }

    setDocsHint("取得中…");
    try {
      const res = await getJSON(
        `/api/knowledge/datasets/${encodeURIComponent(state.datasetId)}/documents`
      );
      if (token !== state.loadToken) return;

      const docs = res.items || [];
      state.docs = docs;

      const filtered = keywordFilter(docs, docKeyword?.value);
      setDocsSummary(`docs=${filtered.length}`);
      setDocsHint("OK");
      renderDocs(filtered);
    } catch (e) {
      if (token !== state.loadToken) return;
      setDocsHint(`ERR: ${String(e?.message || e)}`);
      renderDocs([]);
    }
  }

  function openSegModal(datasetId, docId, docName) {
    state.seg.open = true;
    state.seg.datasetId = datasetId;
    state.seg.docId = docId;
    state.seg.docName = docName;
    state.seg.page = 1;
    state.seg.hasMore = false;
    state.seg.total = 0;

    if (segTitle) segTitle.textContent = `Chunks: ${docName}`;
    if (segDetail) segDetail.textContent = "";
    if (segKeyword) segKeyword.value = "";
    if (segStatus) segStatus.value = "";

    segModal?.setAttribute("aria-hidden", "false");
    segModal?.classList.add("show");
    document.body.classList.add("modalOpen");

    loadSegments();
  }

  function closeSegModal() {
    state.seg.open = false;
    segModal?.setAttribute("aria-hidden", "true");
    segModal?.classList.remove("show");
    document.body.classList.remove("modalOpen");
  }

  async function loadSegments() {
    if (!state.seg.open) return;

    const ds = state.seg.datasetId;
    const doc = state.seg.docId;

    const kw = (segKeyword?.value || "").trim();
    const st = (segStatus?.value || "").trim();
    const page = state.seg.page;
    const limit = state.seg.limit;

    if (segTbody) segTbody.innerHTML = "";
    if (segDetail) segDetail.textContent = "";
    if (segPageInfo) segPageInfo.textContent = `page=${page}`;
    if (segTotalInfo) segTotalInfo.textContent = "";

    try {
      const q = new URLSearchParams();
      q.set("page", String(page));
      q.set("limit", String(limit));
      if (kw) q.set("keyword", kw);
      if (st) q.set("status", st);

      const res = await getJSON(
        `/api/knowledge/datasets/${encodeURIComponent(ds)}/documents/${encodeURIComponent(
          doc
        )}/segments?${q.toString()}`
      );

      const items = res.items || [];
      state.seg.hasMore = !!res.has_more;
      state.seg.total = Number(res.total || 0);

      if (segTotalInfo) segTotalInfo.textContent = `total=${state.seg.total || "-"}`;
      if (segPrevBtn) segPrevBtn.disabled = page <= 1;
      if (segNextBtn) segNextBtn.disabled = !state.seg.hasMore;

      if (!items.length) {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td colspan="4" class="kEmpty">(チャンクなし)</td>`;
        segTbody?.appendChild(tr);
        return;
      }

      for (const it of items) {
        const sid = it?.id || "";
        const idx = it?.position ?? it?.index ?? "-";
        const tk = it?.tokens ?? it?.token_count ?? "-";
        const prev = (it?.content || it?.text || "")
          .slice(0, 140)
          .replace(/\s+/g, " ");

        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${escapeHtml(String(sid))}</td>
          <td>${escapeHtml(String(idx))}</td>
          <td>${escapeHtml(String(tk))}</td>
          <td><a href="#" class="segLink" data-sid="${escapeHtml(String(sid))}">${escapeHtml(
          prev
        )}</a></td>
        `;
        segTbody?.appendChild(tr);
      }

      segTbody?.querySelectorAll("a.segLink").forEach((a) => {
        a.addEventListener("click", async (e) => {
          e.preventDefault();
          const sid = a.getAttribute("data-sid") || "";
          if (!sid) return;

          try {
            const d = await getJSON(
              `/api/knowledge/datasets/${encodeURIComponent(ds)}/documents/${encodeURIComponent(
                doc
              )}/segments/${encodeURIComponent(sid)}`
            );
            const body = d.item?.content || d.item?.text || "";
            if (segDetail) segDetail.textContent = body || "(empty)";
          } catch (err) {
            if (segDetail) segDetail.textContent = `ERR: ${String(err?.message || err)}`;
          }
        });
      });
    } catch (e) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="4" class="kEmpty">ERR: ${escapeHtml(String(e?.message || e))}</td>`;
      segTbody?.appendChild(tr);
    }
  }

  function bindEvents() {
    reloadDatasetsBtn?.addEventListener("click", async () => {
      await loadDatasets();
      await loadDocuments();
    });

    datasetSel?.addEventListener("change", async () => {
      state.datasetId = datasetSel.value;
      await loadDocuments();
    });

    refreshBtn?.addEventListener("click", async () => {
      await loadDocuments();
    });

    clearBtn?.addEventListener("click", async () => {
      if (docKeyword) docKeyword.value = "";
      await loadDocuments();
    });

    docKeyword?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") loadDocuments();
    });

    segClose?.addEventListener("click", closeSegModal);
    segOk?.addEventListener("click", closeSegModal);
    segModal?.addEventListener("click", (e) => {
      if (e.target === segModal) closeSegModal();
    });

    segSearchBtn?.addEventListener("click", () => {
      state.seg.page = 1;
      loadSegments();
    });

    segPrevBtn?.addEventListener("click", () => {
      if (state.seg.page > 1) {
        state.seg.page -= 1;
        loadSegments();
      }
    });

    segNextBtn?.addEventListener("click", () => {
      if (state.seg.hasMore) {
        state.seg.page += 1;
        loadSegments();
      }
    });
  }

  async function boot() {
    await bootNoticeModal();
    bindEvents();
    await loadDatasets();
    await loadDocuments();
  }

  boot();
})();
