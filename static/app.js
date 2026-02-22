// static/app.js
(() => {
    const $ = (id) => document.getElementById(id);

    const inputDir = $("inputDir");
    const outputDir = $("outputDir");
    const recursive = $("recursive");

    const user = $("user");
    const style = $("style");
    const chunkSep = $("chunkSep");
    const overwrite = $("overwrite");

    const scanBtn = $("scanBtn");
    const runBtn = $("runBtn");

    const fileList = $("fileList");
    const log = $("log");

    function addLog(line, kind = "info") {
        const ts = new Date().toLocaleTimeString();
        const prefix = kind === "bad" ? "[ERR]" : kind === "ok" ? "[OK]" : "[..]";
        log.textContent += `${ts} ${prefix} ${line}\n`;
        log.scrollTop = log.scrollHeight;
    }

    function setFiles(files) {
        fileList.textContent = (files && files.length)
            ? files.map((f) => `- ${f}`).join("\n")
            : "(対象なし)";
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

    async function getHealth() {
        try {
            const r = await fetch("/api/health");
            const data = await r.json();
            if (!data.api_ready) {
                addLog(".env の DIFY_API_BASE / DIFY_API_KEY が未設定です。", "bad");
            }
        } catch {
            // noop
        }
    }

    scanBtn.addEventListener("click", async () => {
        log.textContent = "";
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
        log.textContent = "";
        addLog("変換開始（SSE）");
        runBtn.disabled = true;
        scanBtn.disabled = true;

        try {
            const payload = {
                input_dir: inputDir.value.trim(),
                output_dir: outputDir.value.trim(),
                recursive: recursive.checked,

                user: (user?.value || "rag_converter").trim() || "rag_converter",
                knowledge_style: style?.value || "rag_markdown",
                chunk_sep: (chunkSep?.value || "***").trim() || "***",

                overwrite: !!overwrite?.checked,
            };

            const r = await fetch("/api/run", {
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
                    try { obj = JSON.parse(dataLine); } catch { obj = { raw: dataLine }; }

                    if (ev === "meta") {
                        const ow = obj.overwrite ? "ON" : "OFF";
                        addLog(`総件数: ${obj.total} / 上書き: ${ow}`);
                    } else if (ev === "progress") {
                        addLog(`(${obj.index}/${obj.total}) ${obj.file}`);
                    } else if (ev === "skip_one") {
                        addLog(`スキップ: ${obj.file}（既存）`, "ok");
                    } else if (ev === "done_one") {
                        addLog(`保存: ${obj.file} -> ${obj.out}`, "ok");
                    } else if (ev === "error_one") {
                        addLog(`失敗: ${obj.file} / ${obj.error}`, "bad");
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
        }
    });

    getHealth();
})();