/* Durable batch quotes: the browser only creates tasks and reads progress. */
document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById(id);
    const historyPage = !!$('auto-quote-tasks');
    if (!historyPage && $('quote-mode')?.value !== 'auto') return;
    const labels = {queued:'等待执行', running:'询价中', completed:'已结束', stopped:'已停止', pending:'等待中', success:'报价完整', partial:'部分报价', failed:'失败', no_quote:'无可用报价', cancelled:'已停止'};
    const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const date = value => value ? new Date(value).toLocaleString() : '—';
    const csrf = document.querySelector('[name=csrfmiddlewaretoken]').value;
    let batchPage = 1, itemPage = 1, addressPage = 1, selectedBatch = new URLSearchParams(location.search).get('batch');
    let groups = {}, refreshing = false, startToken = null, startSignature = null;
    const uuid = () => crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const n = Math.floor(Math.random() * 16); return (c === 'x' ? n : (n & 3) | 8).toString(16);
    });
    const endpoint = args => {
        const url = new URL('/post_nsop/', location.origin);
        for (const [key, value] of Object.entries(args)) url.searchParams.set(key, value);
        return url;
    };
    function message(text, error = false) {
        $('auto-ui-message').hidden = false;
        $('auto-ui-message').className = 'alert ' + (error ? 'alert-danger' : 'alert-info');
        $('auto-ui-message').textContent = text;
    }
    async function decode(response) {
        if (response.redirected) throw new Error('登录已过期，请刷新页面后重新登录');
        let body;
        try { body = await response.json(); } catch (_) { throw new Error('服务器暂时无法响应，请稍后刷新任务状态'); }
        if (!response.ok || !body.success) throw new Error([body.message || '请求失败', ...(body.errors || [])].join('\n'));
        return body;
    }
    const get = args => fetch(endpoint({step:'auto_quote_data', ...args}), {cache:'no-store'}).then(decode);
    async function post(step, values) {
        const data = new FormData();
        data.append('step', step); data.append('csrfmiddlewaretoken', csrf);
        Object.entries(values).forEach(([key, value]) => data.append(key, value));
        return decode(await fetch(endpoint({}), {method:'POST', body:data}));
    }
    function paginate(prefix, data) {
        $(prefix + '-page').textContent = `${data.page} / ${data.pages} 页`;
        $(prefix + '-prev').disabled = data.page <= 1;
        $(prefix + '-next').disabled = data.page >= data.pages;
    }
    function progress(batch) {
        return Object.entries(batch.counts).map(([key, value]) => `${labels[key] || key} ${value}`).join(' / ');
    }
    function groupCount() {
        if ($('auto-group-count')) $('auto-group-count').textContent = `共 ${groups[$('auto-group').value] || 0} 条地址`;
    }
    async function refresh() {
        if (refreshing) return;
        refreshing = true;
        try {
            const data = await get({page:batchPage});
            groups = data.groups; groupCount(); batchPage = data.page;
            if (!historyPage) return;
            $('auto-worker-state').textContent = data.worker_online ? '后台执行服务在线 · 页面每5秒更新进度' : '后台执行服务尚未就绪，已提交任务会保留在队列中。请联系管理员检查执行服务。';
            $('auto-batches').innerHTML = data.batches.map(batch => `<tr>
                <td>#${batch.id}<div class="small">比较编号：${esc(batch.profile_code)}</div>${batch.profile_id ? `<a href="${endpoint({step:'auto_quote_analysis',profile:batch.profile_id})}">价格分析</a>` : ''}${batch.parent_id ? `<div class="small text-muted">重试自 #${batch.parent_id}</div>` : ''}</td>
                <td>${esc(batch.group)} / ${esc(batch.origin)}</td><td>${esc(batch.operator)}</td><td>${esc(date(batch.created_at))}</td>
                <td>${esc(labels[batch.status])}${batch.stop_requested && batch.status === 'running' ? '（正在停止，等待当前询价结束）' : ''}<div class="small">共${batch.total}条 · ${esc(progress(batch))}</div></td>
                <td><button class="btn btn-sm btn-outline-primary" data-action="detail" data-id="${batch.id}">查看</button>
                ${['queued','running'].includes(batch.status) && !batch.stop_requested ? `<button class="btn btn-sm btn-outline-danger" data-action="stop" data-id="${batch.id}">停止后续询价</button>` : ''}
                ${['completed','stopped'].includes(batch.status) && ['failed','partial','no_quote','cancelled'].some(key => batch.counts[key]) ? `<button class="btn btn-sm btn-outline-secondary" data-action="retry" data-id="${batch.id}">重试未完成项</button>` : ''}</td></tr>`).join('') || '<tr><td colspan="6" class="text-muted">暂无自动询价任务</td></tr>';
            paginate('auto-batches', data);
            if (selectedBatch) await detail();
        } catch (error) { message(error.message, true); }
        finally { refreshing = false; }
    }
    function rows(value) {
        if (!value || typeof value !== 'object') return [];
        for (const key of ['rates','quotes']) if (Array.isArray(value[key])) return value[key];
        for (const child of Object.values(value)) { const found = rows(child); if (found.length) return found; }
        return [];
    }
    function sortedQuotes(carrier) {
        return rows(carrier).filter(q => q && typeof q === 'object').map(q => {
            const value = q.TotalQuote ?? q.totalPrice ?? q.price;
            const valid = (typeof value === 'number' || typeof value === 'string') &&
                String(value).trim() !== '' && Number.isFinite(Number(value)) && Number(value) >= 0;
            return {quote: q, price: valid ? Number(value) : null};
        }).sort((a, b) => (a.price ?? Infinity) - (b.price ?? Infinity));
    }
    const quoteName = q => q.DisplayService || q.carrierName || q.serviceName || q.carrierCode || q.Service || '报价';
    function prices(carrier, {limit = Infinity, key = '', open = false} = {}) {
        const quotes = sortedQuotes(carrier);
        const minimum = quotes.length ? quotes[0].price : null;
        const lines = quotes.map(({quote: q, price}) => {
            const lowest = price !== null && price === minimum;
            return `<div>${esc(quoteName(q))} <strong${lowest ? ' style="color:#dc3545" title="本平台最低价"' : ''}>${price !== null ? '$' + price.toFixed(2) : '价格待返回'}${lowest ? '（最低）' : ''}</strong></div>`;
        });
        if (lines.length > limit) {
            return lines.slice(0, limit).join('') + `<details data-id="${esc(key)}" ${open ? 'open' : ''}><summary class="text-primary mt-1">其余 ${lines.length - limit} 条报价（共 ${lines.length} 条，点击展开 / 收起）</summary>${lines.slice(limit).join('')}</details>`;
        }
        return lines.join('') || esc(carrier?.error || '暂无报价');
    }
    function priceText(carrier) {
        const quotes = sortedQuotes(carrier);
        return quotes.map(({quote, price}) => `${quoteName(quote)} ${price === null ? '价格待返回' : '$' + price.toFixed(2)}${price !== null && price === quotes[0].price ? '（最低）' : ''}`).join('\n') || carrier?.error || '暂无报价';
    }
    function copyTable(data) {
        const headers = ['收货地址', '状态 / 实际询价时间', 'Maersk', '卡卡省', 'ABF', '明细 / 错误'];
        const cells = data.rows.map(item => {
            const a = item.address, result = item.result.results || {};
            return [`${a.city}, ${a.state} ${a.zipcode}\n${a.address}\n${a.distance_miles || ''} miles`,
                `${labels[item.status] || item.status}\n${date(item.started_at)}`, priceText(result.maersk), priceText(result.kakas),
                rows(result.abf).length ? priceText(result.abf) : '暂未接入',
                [item.error, item.finished_at ? '完成时间：' + date(item.finished_at) : ''].filter(Boolean).join('\n')];
        });
        // Keep pasted text as text, including addresses/names that start with spreadsheet formulas.
        const safe = value => /^[\s]*[=+\-@]/.test(String(value)) ? "'" + value : String(value ?? '');
        const grid = [headers, ...cells].map(row => row.map(safe));
        return {
            text: grid.map(row => row.map(value => '"' + value.replace(/"/g, '""') + '"').join('\t')).join('\r\n'),
            html: '<html><body><table border="1" style="border-collapse:collapse">' + grid.map((row, index) => '<tr>' + row.map(value => {
                const tag = index === 0 ? 'th' : 'td';
                return `<${tag} style="vertical-align:top;white-space:pre-wrap">${esc(value).replace(/\r?\n/g, '<br style="mso-data-placement:same-cell">')}</${tag}>`;
            }).join('') + '</tr>').join('') + '</table></body></html>',
            count: cells.length,
        };
    }
    function legacyCopy(payload) {
        const area = document.createElement('textarea');
        area.value = payload.text;
        area.style.cssText = 'position:fixed;left:-10000px;top:0';
        document.body.appendChild(area);
        area.select();
        const onCopy = event => {
            if (!event.clipboardData) return;
            event.clipboardData.setData('text/html', payload.html);
            event.clipboardData.setData('text/plain', payload.text);
            event.preventDefault();
        };
        document.addEventListener('copy', onCopy);
        try { return document.execCommand('copy'); }
        finally { document.removeEventListener('copy', onCopy); area.remove(); }
    }
    async function detail() {
        const requestedId = selectedBatch;
        const data = await get({kind:'batch', batch:requestedId, page:itemPage, status:$('auto-item-status').value});
        if (requestedId !== selectedBatch) return;
        $('auto-detail').hidden = false;
        $('auto-detail-title').textContent = `任务 #${data.batch.id} · ${data.batch.group} · ${data.batch.origin}`;
        $('auto-detail-summary').textContent = `执行人：${data.batch.operator} · ${progress(data.batch)}`;
        $('auto-detail-parameters').textContent = JSON.stringify(data.parameters, null, 2);
        $('auto-export').href = endpoint({step:'auto_quote_export', batch:selectedBatch});
        // Preserve opened details during polling.
        const opened = new Set(Array.from(document.querySelectorAll('#auto-items details[open]')).map(el => el.dataset.id));
        $('auto-items').innerHTML = data.rows.map(item => {
            const a = item.address, result = item.result.results || {};
            return `<tr><td>${esc(a.city)}, ${esc(a.state)} ${esc(a.zipcode)}<div>${esc(a.address)}</div><small>${esc(a.distance_miles)} miles</small></td>
                <td>${esc(labels[item.status])}<div class="small">${esc(date(item.started_at))}</div></td>
                <td>${prices(result.maersk)}</td><td>${prices(result.kakas, {limit:10, key:'kakas-' + item.id, open:opened.has('kakas-' + item.id)})}</td><td>${rows(result.abf).length ? prices(result.abf) : '暂未接入'}</td>
                <td><div class="text-danger" style="max-width:320px;overflow-wrap:anywhere">${esc(item.error)}</div>
                <details data-id="${item.id}" ${opened.has(String(item.id)) ? 'open' : ''}><summary>完整结果与请求</summary><pre style="max-width:400px;max-height:300px;white-space:pre-wrap">${esc(JSON.stringify({address:a, request:item.request_payload, result:item.result, finished_at:item.finished_at}, null, 2))}</pre></details></td></tr>`;
        }).join('') || '<tr><td colspan="6">没有符合条件的记录</td></tr>';
        itemPage = data.page; paginate('auto-items', data);
    }
    async function addresses() {
        const group = $('auto-group').value;
        const data = await get({kind:'addresses', group, page:addressPage});
        if (group !== $('auto-group').value) return;
        $('auto-address-list').hidden = false;
        $('auto-address-rows').innerHTML = data.rows.map(a => `<tr>${[a.group,a.city,a.state,a.zipcode,a.address,a.distance_miles].map(v => `<td>${esc(v)}</td>`).join('')}</tr>`).join('') || '<tr><td colspan="6">该组还没有地址</td></tr>';
        addressPage = data.page; paginate('auto-address', data);
    }
    const safely = action => async () => { try { await action(); } catch (error) { message(error.message, true); } };
    let preparedCopy = null;
    $('auto-copy-table')?.addEventListener('click', async () => {
        if (!selectedBatch) return;
        const batchId = String(selectedBatch), button = $('auto-copy-table');
        button.disabled = true;
        button.textContent = '正在准备完整表格…';
        const payloadPromise = preparedCopy?.batchId === batchId ? Promise.resolve(preparedCopy.payload) :
            get({kind:'batch_copy', batch:batchId}).then(copyTable);
        // Handle a possible request rejection even if ClipboardItem construction fails first.
        payloadPromise.catch(() => {});
        try {
            if (navigator.clipboard?.write && typeof ClipboardItem !== 'undefined') {
                // Start the write during the click gesture; the blobs resolve after fetching all rows.
                const htmlBlob = payloadPromise.then(p => new Blob([p.html], {type:'text/html'}));
                const textBlob = payloadPromise.then(p => new Blob([p.text], {type:'text/plain'}));
                htmlBlob.catch(() => {}); textBlob.catch(() => {});
                const item = new ClipboardItem({'text/html':htmlBlob, 'text/plain':textBlob});
                try { await navigator.clipboard.write([item]); }
                catch (_) { if (!legacyCopy(await payloadPromise)) throw new Error('浏览器未允许复制'); }
            } else if (!legacyCopy(await payloadPromise)) {
                throw new Error('浏览器未允许复制');
            }
            const payload = await payloadPromise;
            preparedCopy = null;
            message(`已复制任务 #${batchId} 的全部 ${payload.count} 条地址及完整报价，可以粘贴到 Excel；不受分页或状态筛选影响。`);
        } catch (error) {
            try {
                preparedCopy = {batchId, payload:await payloadPromise};
                message('完整表格已准备好，但复制被浏览器阻止，请再次点击“复制整个表”。', true);
            } catch (_) { message(error.message, true); }
        } finally {
            button.disabled = false;
            button.textContent = '复制整个表（完整报价）';
        }
    });
    $('auto-refresh')?.addEventListener('click', refresh);
    $('auto-batches')?.addEventListener('click', async event => {
        const button = event.target.closest('button[data-action]');
        if (!button) return;
        button.disabled = true;
        try {
            if (button.dataset.action === 'detail') {
                selectedBatch = button.dataset.id; itemPage = 1; $('auto-item-status').value = ''; await detail();
                $('auto-detail').scrollIntoView({behavior:'smooth', block:'start'});
            } else {
                button.dataset.token ||= uuid();
                const data = await post('auto_quote_' + button.dataset.action, {batch:button.dataset.id, submission_id:button.dataset.token});
                selectedBatch = String(data.batch.id); itemPage = 1;
                message(button.dataset.action === 'stop' ? '已停止后续询价，正在执行的地址会完成并保存。' : `已创建重试任务 #${data.batch.id}，原结果保留。`);
                await refresh();
            }
        } catch (error) { message(error.message, true); }
        finally { button.disabled = false; }
    });
    for (const [suffix, delta] of [['prev',-1],['next',1]]) {
        $('auto-batches-' + suffix)?.addEventListener('click', () => { batchPage += delta; refresh(); });
        $('auto-items-' + suffix)?.addEventListener('click', safely(async () => { itemPage += delta; await detail(); }));
        $('auto-address-' + suffix)?.addEventListener('click', safely(async () => { addressPage += delta; await addresses(); }));
    }
    $('auto-item-status')?.addEventListener('change', safely(async () => { itemPage = 1; await detail(); }));
    $('auto-show-addresses')?.addEventListener('click', safely(addresses));
    $('auto-group')?.addEventListener('change', () => {
        groupCount(); addressPage = 1; $('auto-address-list').hidden = true;
        $('auto-import-message').textContent = ''; startToken = null;
    });
    $('auto-upload')?.addEventListener('click', async () => {
        const file = $('auto-address-file').files[0];
        if (!file) { $('auto-import-message').textContent = '请选择Excel文件'; return; }
        const group = $('auto-group').value;
        $('auto-upload').disabled = true;
        $('auto-import-message').textContent = `正在导入 ${group} 组…`;
        try {
            const data = await post('auto_quote_import', {group, file});
            $('auto-import-message').textContent = `${group} 组：新增 ${data.added} 条，跳过重复 ${data.duplicates} 条。`;
            $('auto-address-file').value = '';
            await refresh();
            if (!$('auto-address-list').hidden) await addresses();
        } catch (error) { $('auto-import-message').textContent = error.message; }
        finally { $('auto-upload').disabled = false; }
    });
    window.AutoQuoteUI = {
        refresh, prices, rows, esc, date, labels, get,
        clearSelection() { selectedBatch = null; $('auto-detail').hidden = true; },
        async showBatch(id) { selectedBatch = String(id); itemPage = 1; $('auto-item-status').value = ''; await detail(); $('auto-detail').scrollIntoView({behavior:'smooth', block:'start'}); },
        async start(payload) {
            const group = $('auto-group').value;
            const signature = JSON.stringify({group, payload});
            if (!startToken || signature !== startSignature) { startToken = uuid(); startSignature = signature; }
            const data = await post('auto_quote_start', {group, quote_payload:JSON.stringify(payload), submission_id:startToken});
            startToken = null; selectedBatch = String(data.batch.id); itemPage = 1; batchPage = 1;
            message(`任务 #${data.batch.id} 已提交，共 ${data.batch.total} 条地址。可以关闭页面，稍后到自动历史查看。`);
            await refresh();
        }
    };
    if (!$('quote-mode') || $('quote-mode').value === 'auto') refresh();
    setInterval(() => {
        if (historyPage && !document.hidden && (!$('quote-mode') || $('quote-mode').value === 'auto')) refresh();
    }, 5000);
});
