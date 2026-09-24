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
    async function load() {
        const token = ++generation;
        $('destination-history').hidden = false;
        $('destination-message').textContent = '正在查询地址历史…';
        $('destination-rows').innerHTML = '';
        $('destination-prev').disabled = true; $('destination-next').disabled = true;
        try {
            const data = await get({kind:'destination_history', ...filters, page});
            if (token !== generation || !active) return;
            $('auto-task-list').hidden = false;
            $('destination-message').textContent = `共 ${data.total} 条历史记录，匹配 ${data.addresses.length} 个收货地址。`;
            options('destination-address', data.addresses.map(a => ({value:a.key,label:a.label})), data.address, '全部匹配地址');
            filters.address = data.address;
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
    $('destination-address').addEventListener('change', () => { filters.address = $('destination-address').value; page = 1; load(); });
    for (const [key, delta] of [['prev',-1],['next',1]]) $('destination-'+key).addEventListener('click', () => { page += delta; load(); });
    $('destination-rows').addEventListener('click', async event => {
        const button = event.target.closest('button[data-batch]');
        if (!button) return;
        try { await ui.showBatch(button.dataset.batch); }
        catch (error) { $('destination-message').textContent = error.message; }
    });
});
