(() => {
    'use strict';
    const table = document.getElementById('shipped-table');
    const form = document.getElementById('shipped-edit-form');
    const dialog = document.getElementById('shipped-edit-dialog');
    const save = document.getElementById('shipped-save');
    const message = document.getElementById('shipped-edit-message');
    const pageMessage = document.getElementById('shipped-page-message');
    let saving = false;
    let opening = false;
    const editable = ['plt_ids', 'order_id', 'revision', 'customer_name', 'container', 'destination', 'address', 'zipcode', 'delivery_method', 'delivery_type', 'location',
        'offload_at', 'shipped_at', 'shipping_mark', 'fba_id', 'ref_id', 'weight', 'pcs', 'cbm', 'n_pallet', 'note', 'shipment', 'appointment_id'];

    const rows = Array.from(table.tBodies[0].rows).filter(row => !row.classList.contains('shipped-empty'))
        .map(row => ({row, text: Array.from(row.cells, cell => cell.textContent.trim().toUpperCase())}));
    const filters = Array.from(document.querySelectorAll('#shipped-filters input'))
        .map(input => ({input, column: input.closest('th').cellIndex}));
    let timer;
    function filterRows() {
        const active = filters.map(({input, column}) => ({column, value: input.value.trim().toUpperCase()})).filter(item => item.value);
        const totals = [0, 0, 0, 0];
        let visibleCount = 0;
        for (const {row, text} of rows) {
            const visible = active.every(({column, value}) => text[column].includes(value));
            row.hidden = !visible;
            if (visible) {
                visibleCount++;
                [10, 11, 12, 13].forEach((column, i) => {totals[i] += Number(text[column]) || 0;});
            }
        }
        ['weight', 'pcs', 'cbm', 'pallets'].forEach((name, i) => {
            document.getElementById(`shipped-total-${name}`).textContent = totals[i].toFixed(i === 0 ? 4 : i === 2 ? 2 : 0);
        });
        document.getElementById('shipped-visible-count').textContent = visibleCount;
        document.getElementById('shipped-filter-empty').hidden = rows.length === 0 || visibleCount !== 0;
    }
    filters.forEach(({input}) => input.addEventListener('input', () => {clearTimeout(timer); timer = setTimeout(filterRows, 120);}));
    document.getElementById('shipped-clear-filters').addEventListener('click', () => {
        clearTimeout(timer);
        filters.forEach(({input}) => {input.value = '';});
        filterRows();
    });
    filterRows();
    // 按实际列宽定位冻结列，适配不同屏幕宽度与浏览器缩放。
    if (typeof ResizeObserver !== 'undefined') {
        new ResizeObserver(() => {
            const cells = table.tHead.rows[0].cells;
            const firstWidth = cells[0].getBoundingClientRect().width;
            table.style.setProperty('--ship-customer-left', `${firstWidth}px`);
            table.style.setProperty('--ship-container-left', `${firstWidth + cells[1].getBoundingClientRect().width}px`);
        }).observe(table);
    }

    async function post(data) {
        const response = await fetch(window.location.pathname, {method: 'POST', body: data, credentials: 'same-origin', headers: {'Accept': 'application/json'}});
        if (response.redirected || !(response.headers.get('Content-Type') || '').includes('application/json')) {
            throw new Error('服务器未返回操作结果，请检查登录状态，刷新核对后再试。');
        }
        const result = await response.json();
        if (!response.ok || !result.success) throw new Error(result.message || '操作失败，请重试。');
        return result;
    }

    table.addEventListener('click', async event => {
        const button = event.target.closest('.shipped-edit');
        if (!button || opening || saving) return;
        opening = true;
        button.disabled = true;
        const label = button.textContent;
        button.textContent = '读取中…';
        pageMessage.hidden = true;
        try {
            const data = new FormData(form);
            data.set('step', 'edit_data');
            data.set('plt_ids', button.dataset.ids);
            data.set('order_id', button.dataset.order);
            const result = (await post(data)).data;
            form.reset();
            ['delivery_method', 'delivery_type'].forEach(name => {
                const select = form.elements[name];
                select.querySelectorAll('[data-current]').forEach(option => option.remove());
                if (!Array.from(select.options).some(option => option.value === result[name])) {
                    const option = new Option(result[name], result[name]);
                    option.dataset.current = 'true';
                    select.add(option);
                }
            });
            editable.forEach(name => {form.elements[name].value = result[name] ?? '';});
            document.getElementById('shipped-shared-notice').textContent =
                `本行汇总 ${result.n_pallet} 块卡板。目的地、地址、邮编、派送方式及派送类型同步更新同柜号、同 PO_ID 的装箱单。客户、柜号、时间和预约信息展示关联记录的原值。`;
            message.hidden = true;
            dialog.showModal();
        } catch (error) {
            pageMessage.textContent = error.message;
            pageMessage.hidden = false;
        } finally {
            opening = false;
            button.disabled = false;
            button.textContent = label;
        }
    });
    document.querySelectorAll('.shipped-close').forEach(button => button.addEventListener('click', () => {if (!saving) dialog.close();}));
    dialog.addEventListener('cancel', event => {if (saving) event.preventDefault();});
    form.addEventListener('submit', async event => {
        event.preventDefault();
        if (saving || !form.reportValidity()) return;
        saving = true;
        save.disabled = true;
        save.textContent = '保存中…';
        message.hidden = true;
        try {
            const data = new FormData(form);
            const result = await post(data);
            message.className = 'alert alert-success';
            message.textContent = result.message;
            message.hidden = false;
            save.textContent = '已保存，正在刷新…';
            // 重载当前 GET 查询，保留仓库和派送类型，避免重复提交保存请求。
            window.location.reload();
        } catch (error) {
            message.className = 'alert alert-danger';
            message.textContent = error instanceof TypeError ? '网络连接中断，无法确认保存结果，请刷新核对后重试。' : error.message;
            message.hidden = false;
            saving = false;
            save.disabled = false;
            save.textContent = '保存修改';
        }
    });
})();
