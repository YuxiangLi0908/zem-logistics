/* Cross-task address history. Rank lines deliberately do not imply carrier identity. */
document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById(id), ui = window.AutoQuoteUI;
    if (!$('destination-search') || !ui) return;
    const {esc, date, prices, rows, labels, get} = ui;
    let filters = {}, page = 1, generation = 0, active = false;
    const colors = ['#dc2626','#2563eb','#059669','#9333ea','#d97706','#0891b2','#be185d','#475569','#4f46e5','#4d7c0f'];
    function options(id, values, selected, placeholder) {
        $(id).innerHTML = (placeholder ? `<option value="">${esc(placeholder)}</option>` : '') +
            values.map(v => `<option value="${esc(v.value)}">${esc(v.label)}</option>`).join('');
        $(id).value = String(selected ?? '');
    }
    function chart(data) {
        $('destination-chart-note').textContent = (data.chart_message || '') +
            (data.chart_limited ? ' 图表仅使用该编号最近1000次记录；可缩小日期范围，下方历史表仍保留全部匹配记录。' : '') +
            ' 接口未标币种时按 USD 记录；点上会标注。价格是返回的报价条目，同一承运商不同服务可占多个名次。';
        const points = data.chart || [], valid = points.flatMap(p => p.prices.map(q => Number(q.price)));
        $('destination-legend').innerHTML = '';
        if (!valid.length) { $('destination-chart').textContent = '当前条件没有可绘制的价格。'; return; }
        const width = 1050, height = 380, left = 85, right = 35, top = 25, bottom = 65;
        const start = new Date(points[0].time).getTime(), end = new Date(points.at(-1).time).getTime();
        const max = Math.max(...valid) * 1.1 || 1;
        const x = p => left + (end === start ? 0.5 : (new Date(p.time).getTime() - start) / (end - start)) * (width - left - right);
        const y = value => height - bottom - Number(value) / max * (height - top - bottom);
        let svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="每次询价最低前十名价格随时间变化" style="width:100%;min-width:700px"><title>最低10价走势（${esc(data.currency)}）</title>`;
        for (let i = 0; i <= 4; i++) {
            const value = max * i / 4, py = y(value);
            svg += `<line x1="${left}" x2="${width-right}" y1="${py}" y2="${py}" stroke="#e2e8f0"/><text x="${left-8}" y="${py+4}" text-anchor="end" font-size="12">${value.toFixed(2)}</text>`;
        }
        svg += `<text x="${left}" y="15" font-size="12">${esc(data.currency)}</text>`;
        const tickIndexes = [...new Set([0, Math.floor((points.length-1)/2), points.length-1])];
        for (const index of tickIndexes) {
            const p = points[index];
            svg += `<text x="${x(p)}" y="${height-35}" text-anchor="${index === 0 ? 'start' : index === points.length-1 ? 'end' : 'middle'}" font-size="11">${esc(String(p.time).slice(0,10))}</text>`;
        }
        for (let rank = 0; rank < 10; rank++) {
            let path = '', connected = false, circles = '';
            for (const p of points) {
                const q = p.prices[rank];
                if (!q) { connected = false; continue; }
                const px = x(p), py = y(q.price);
                path += `${connected ? 'L' : 'M'}${px},${py} `; connected = true;
                const tooltip = `取件日期 ${String(p.time).slice(0,10)} · 任务 #${p.batch_id}\n实际询价：${date(p.queried_at)}\n第${rank+1}低：${q.price} ${data.currency}\n${q.platform} / ${q.carrier} / ${q.service || '未标注服务'}${q.platform_complete ? '' : '\n部分返回报价'}${q.currency_assumed ? '\n接口未标币种，按USD记录' : ''}`;
                circles += `<circle cx="${px}" cy="${py}" r="4" tabindex="0" aria-label="${esc(tooltip)}"><title>${esc(tooltip)}</title></circle>`;
            }
            if (!circles) continue;
            svg += `<g data-rank="${rank}" fill="${colors[rank]}"><path d="${path}" fill="none" stroke="${colors[rank]}" stroke-width="1.8"/>${circles}</g>`;
            $('destination-legend').insertAdjacentHTML('beforeend', `<label style="color:${colors[rank]}"><input type="checkbox" data-rank="${rank}" checked> 第${rank+1}低</label>`);
        }
        $('destination-chart').innerHTML = svg + '</svg>';
    }
    async function load() {
        const token = ++generation;
        $('destination-history').hidden = false;
        $('destination-message').textContent = '正在查询地址历史…';
        $('destination-rows').innerHTML = '';
        $('destination-chart').textContent = ''; $('destination-legend').innerHTML = '';
        $('destination-analysis').hidden = true;
        $('destination-prev').disabled = true; $('destination-next').disabled = true;
        try {
            const data = await get({kind:'destination_history', ...filters, page});
            if (token !== generation || !active) return;
            $('auto-task-list').hidden = false;
            $('destination-message').textContent = `共 ${data.total} 条历史记录，匹配 ${data.addresses.length} 个收货地址。`;
            options('destination-address', data.addresses.map(a => ({value:a.key,label:a.label})), data.address, '全部匹配地址（选择单个地址查看走势）');
            options('destination-profile', data.profiles.map(p => ({value:p.id,label:p.code})), data.profile);
            options('destination-lead', data.leads.map(v => ({value:v,label:v === 'unknown' ? '未知' : v + ' 天'})), data.lead);
            options('destination-currency', data.currencies.map(v => ({value:v,label:v})), data.currency);
            filters.address = data.address;
            if (data.profile) filters.profile = data.profile;
            if (data.lead) filters.lead = data.lead;
            if (data.currency) filters.currency = data.currency;
            $('destination-analysis').hidden = !data.profile;
            $('destination-analysis').href = '/post_nsop/?' + new URLSearchParams({step:'auto_quote_analysis', profile:data.profile || '', address:data.address || ''});
            chart(data);
            $('destination-rows').innerHTML = data.rows.map(item => {
                const a = item.address, result = item.result?.results || {};
                return `<tr><td>#${item.batch_id}<div>${esc(item.operator)}</div><div>${esc(item.origin)}</div><small>${esc(item.profile_code)}</small></td>
                    <td>${esc(a.address)}<div>${esc(a.city)}, ${esc(a.state)} ${esc(a.zipcode)}</div></td>
                    <td>${esc(labels[item.status] || item.status)}<div>${esc(date(item.started_at))}</div><div>取件：${esc(item.pickup_date || "—")}</div><small>创建：${esc(date(item.created_at))}</small></td>
                    <td>${prices(result.maersk)}</td><td>${prices(result.kakas,{limit:10,key:'destination-kakas-'+item.id})}</td><td>${rows(result.abf).length ? prices(result.abf) : '暂无报价'}</td>
                    <td><button type="button" class="btn btn-sm btn-outline-primary" data-batch="${item.batch_id}">查看任务</button><div class="text-danger">${esc(item.error)}</div></td></tr>`;
            }).join('') || '<tr><td colspan="7">没有符合条件的记录</td></tr>';
            page = data.page;
            $('destination-page').textContent = `${page} / ${data.pages} 页`;
            $('destination-prev').disabled = page <= 1;
            $('destination-next').disabled = page >= data.pages;
        } catch (error) {
            if (token === generation) {
                $('destination-message').textContent = error.message;
                $('destination-rows').innerHTML = '';
                $('destination-chart').textContent = ''; $('destination-legend').innerHTML = '';
            }
        }
    }
    $('destination-search').addEventListener('submit', event => {
        event.preventDefault();
        const q = $('destination-query').value.trim();
        if (!q) {
            ++generation; active = false; filters = {}; page = 1;
            $('destination-history').hidden = true;
            $('auto-task-list').hidden = false; ui.clearSelection();
            const start = $('destination-start').value, end = $('destination-end').value;
            $('auto-task-title').textContent = start || end ? '按日期筛选的历史任务' : '全部历史自动询价任务';
            ui.filterTasks(start, end); return;
        }
        $('auto-task-title').textContent = '全部历史自动询价任务';
        ui.filterTasks('', '');
        active = true; page = 1;
        filters = {q, start:$('destination-start').value, end:$('destination-end').value};
        $('destination-platform').value = '';
        ui.clearSelection();
        load();
    });
    $('destination-reset').addEventListener('click', () => {
        ++generation; active = false; filters = {}; page = 1;
        $('destination-search').reset(); $('destination-history').hidden = true;
        $('auto-task-list').hidden = false; ui.clearSelection();
        $('auto-task-title').textContent = '全部历史自动询价任务'; ui.filterTasks('', '');
    });
    $('auto-refresh').addEventListener('click', () => { if (active) load(); });
    for (const key of ['address','profile','lead','platform','currency']) {
        $('destination-'+key).addEventListener('change', () => {
            filters[key] = $('destination-'+key).value;
            if (key === 'address') { delete filters.profile; delete filters.lead; delete filters.currency; }
            if (key === 'profile') { delete filters.lead; delete filters.currency; }
            if (key === 'lead' || key === 'platform') delete filters.currency;
            page = 1; load();
        });
    }
    for (const [key, delta] of [['prev',-1],['next',1]]) $('destination-'+key).addEventListener('click', () => { page += delta; load(); });
    $('destination-legend').addEventListener('change', event => {
        const rank = event.target.dataset.rank;
        if (rank !== undefined) $('destination-chart').querySelector(`[data-rank="${rank}"]`).style.display = event.target.checked ? '' : 'none';
    });
    $('destination-rows').addEventListener('click', async event => {
        const button = event.target.closest('button[data-batch]');
        if (!button) return;
        try { await ui.showBatch(button.dataset.batch); }
        catch (error) { $('destination-message').textContent = error.message; }
    });
});
