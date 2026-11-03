/* Read-only ABF tracking and document retrieval; no booking-mode dependency. */
(() => {
    if (window.ABFFollowup) return;
    const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    let dialog, current, busy = false;
    const api = async (step, fields = {}) => {
        const form = new FormData();
        form.set('step', step);
        form.set('csrfmiddlewaretoken', document.querySelector('[name=csrfmiddlewaretoken]').value);
        Object.entries(fields).forEach(([name,value]) => { if (value != null) form.set(name, value); });
        const response = await fetch('/post_nsop/', {method:'POST', credentials:'same-origin', body:form});
        if (response.redirected) throw new Error('登录已失效，请刷新页面重新登录。');
        let result;
        try { result = await response.json(); }
        catch (_) { throw new Error(`服务器未返回有效查询结果（HTTP ${response.status}），请稍后重试。`); }
        if (!response.ok || !result.success) throw new Error(result.message || `查询失败 HTTP ${response.status}`);
        return result;
    };
    const snapshot = data => `<p class="small text-muted">查询编号：${esc(data.ref_num)}　查询时间：${esc(new Date(data.queried_at).toLocaleString())}</p>`;
    function renderTracking(data) {
        const area = dialog.querySelector('[data-followup-tracking]');
        if (!data) { area.innerHTML = '<p class="text-muted">尚未查询物流。</p>'; return; }
        const shipments = data.shipments || [];
        area.innerHTML = snapshot(data) + (shipments.length ? shipments.map(item => {
            const row = (label, value) => value ? `<div><strong>${label}：</strong>${esc(value)}</div>` : '';
            const exception = [item.pickup_delay_code,item.osd_exception,item.exception_description,item.transit_exception_code,item.transit_exception_description].filter(Boolean).join(' / ');
            return `<article class="border rounded p-3 mb-3" style="white-space:normal;overflow-wrap:anywhere;">
                <h6>PRO：${esc(item.pro_number || '尚未返回')}</h6>
                ${row('当前状态', [item.status_code,item.status].filter(Boolean).join(' / '))}
                ${row('实际提货', [item.pickup_date,item.pickup_time].filter(Boolean).join(' '))}
                ${row('预计送达', item.expected_delivery_date)}${row('应送达日期', item.due_date)}
                ${row('预约送达', [item.appointment_date,item.appointment_time].filter(Boolean).join(' '))}
                ${row('实际送达', [item.delivery_date,item.delivery_time].filter(Boolean).join(' '))}
                ${row('签收人', [item.signature_first_name,item.signature_last_name].filter(Boolean).join(' '))}
                ${row('发货方', [item.shipper_name,item.shipper_city,item.shipper_state].filter(Boolean).join(' '))}
                ${row('收货方', [item.consignee_name,item.consignee_city,item.consignee_state].filter(Boolean).join(' '))}
                ${row('重量 / 件数', [item.weight ? item.weight+' lb' : '',item.pieces ? item.pieces+'件' : ''].filter(Boolean).join(' / '))}
                ${row('BOL编号', (item.bol_numbers || []).join('、'))}${row('PO编号', (item.po_numbers || []).join('、'))}
                ${item.reassigned === 'Y' ? '<div class="text-warning">此票运输已重新分配。</div>' : ''}
                ${exception ? `<div class="alert alert-warning mt-2">延误 / 异常：${esc(exception)}</div>` : ''}
                ${(item.carrier_pros || []).map(carrier => row('关联承运商 / PRO', [carrier.carrier,carrier.pro].join(' / '))).join('')}
                ${item.pro_number ? `<button type="button" class="btn btn-sm btn-outline-primary mt-2" data-documents-pro="${esc(item.pro_number)}">用该 PRO 获取文件</button>` : ''}
            </article>`;
        }).join('') : '<p>ABF 未返回运输记录，请核对编号或稍后再查。</p>');
    }
    function renderDocuments(data) {
        const area = dialog.querySelector('[data-followup-documents]');
        if (!data) { area.innerHTML = '<p class="text-muted">尚未查询文件。</p>'; return; }
        area.innerHTML = snapshot(data) + ((data.documents || []).length ? data.documents.map(item => `<article class="border rounded p-3 mb-3">
            <strong>${esc(item.label || item.type)}（${esc(item.type)}）</strong>
            <div class="d-flex gap-2 flex-wrap mt-2">${(item.pages || []).map(page => `<a class="btn btn-sm btn-outline-primary" href="${esc(page.url)}" target="_blank" rel="noopener noreferrer">打开 / 下载第 ${page.number} 页</a>`).join('') || '暂无可用页'}</div>
        </article>`).join('') : '<p>暂未返回可用文件；文件可能尚未扫描上传，请稍后再查。</p>');
    }
    async function query(kind) {
        if (busy) return;
        const form = dialog.querySelector('form');
        if (!form.reportValidity()) return;
        const fields = Object.fromEntries(new FormData(form));
        if (kind === 'documents' && fields.ref_type === 'K') {
            const error = dialog.querySelector('[data-followup-error]');
            error.textContent = '文件接口不支持提货确认号，请先查询物流，再用返回的 PRO号获取文件。'; error.hidden = false; return;
        }
        const error = dialog.querySelector('[data-followup-error]');
        const area = dialog.querySelector(`[data-followup-${kind}]`);
        error.hidden = true; busy = true;
        dialog.querySelectorAll('button[data-query], button[data-documents-pro]').forEach(button => { button.disabled = true; });
        area.textContent = kind === 'tracking' ? '正在向 ABF 查询物流…' : '正在向 ABF 查询文件…';
        try {
            const result = await api(kind === 'tracking' ? 'abf_tracking' : 'abf_documents', {history_id:current.history_id,...fields});
            if (kind === 'tracking') {
                renderTracking(result.data);
                // A pickup confirmation may resolve to several shipments: retain each PRO's own button.
                const shipments = result.data.shipments || [];
                if (shipments.length === 1 && shipments[0].pro_number) {
                    form.elements.ref_type.value = 'A';
                    form.elements.ref_num.value = shipments[0].pro_number;
                    dialog.querySelector('[data-followup-hint]').textContent = '已带入本票 PRO号，可继续获取文件。';
                }
            } else renderDocuments(result.data);
        } catch (failure) {
            area.textContent = '本次查询未成功，请查看原因后重试。';
            error.textContent = failure.message; error.hidden = false;
        } finally {
            busy = false;
            dialog.querySelectorAll('button[data-query], button[data-documents-pro]').forEach(button => { button.disabled = false; });
        }
    }
    function ensureDialog() {
        if (dialog) return;
        dialog = document.createElement('dialog'); dialog.id = 'abf-followup-dialog';
        dialog.style.cssText = 'width:1000px;max-width:94vw;max-height:90vh;margin:auto;padding:24px;border:1px solid #ddd;border-radius:12px;overflow:auto;';
        dialog.addEventListener('submit', event => { event.preventDefault(); event.stopPropagation(); query('tracking'); });
        dialog.addEventListener('click', event => {
            if (event.target.closest('[data-followup-close]') && !busy) dialog.close();
            const queryButton = event.target.closest('[data-query]');
            if (queryButton) query(queryButton.dataset.query);
            const proButton = event.target.closest('[data-documents-pro]');
            if (proButton && !busy) {
                const form = dialog.querySelector('form');
                form.elements.ref_type.value = 'A'; form.elements.ref_num.value = proButton.dataset.documentsPro;
                query('documents');
            }
        });
        dialog.addEventListener('cancel', event => { if (busy) event.preventDefault(); });
    }
    async function open(historyId, container) {
        if (busy) return;
        ensureDialog(); (container || document.body).appendChild(dialog);
        const data = await api('abf_shipment_preview', {history_id:historyId});
        current = data;
        const refs = data.references || [], first = refs[0] || {type:'A',number:''};
        dialog.innerHTML = `<div class="d-flex justify-content-between"><h4>ABF 物流跟踪 / 文件获取${data.history_id ? ' · 询价 #'+data.history_id : ''}</h4><button type="button" class="btn btn-light" data-followup-close aria-label="关闭">×</button></div>
            <p class="small text-muted">可查询真实运输编号，不受下单测试模式影响。物流时间按 ABF 原值显示；查询不会修改预约或本系统出库状态。</p>
            ${data.booking_status === 'test_success' ? '<div class="alert alert-warning">这条记录是测试下单，没有真实运输。请填写真实 PRO、BOL 等编号进行查询。</div>' : ''}
            ${(data.generated_documents || []).length ? `<div class="mb-3">原下单文件：${data.generated_documents.map(file => `<a class="btn btn-sm btn-outline-secondary me-2" href="${esc(file.url)}" target="_blank" rel="noopener noreferrer">${esc(file.label)}</a>`).join('')}</div>` : ''}
            <form><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;align-items:end;">
            <label>编号类型<select name="ref_type" class="form-select">${Object.entries(data.reference_types).map(([key,label]) => `<option value="${esc(key)}" ${first.type === key ? 'selected' : ''}>${esc(label)}${key === 'K' ? '（仅跟踪）' : ''}</option>`).join('')}</select></label>
            <label>查询编号<input class="form-control" name="ref_num" value="${esc(first.number)}" maxlength="100" required></label>
            <label>文件类型<select name="doc_type" class="form-select">${Object.entries(data.document_types).map(([key,label]) => `<option value="${esc(key)}">${esc(label)}（${esc(key)}）</option>`).join('')}</select></label>
            <label>文件格式<select name="image_type" class="form-select"><option value="L">JPEG 低清</option><option value="H">JPEG 高清</option><option value="T">TIFF</option></select></label></div>
            <div class="d-flex gap-2 mt-3"><button type="button" class="btn btn-primary" data-query="tracking">查询物流</button><button type="button" class="btn btn-outline-primary" data-query="documents">获取文件</button></div></form>
            <p data-followup-hint class="small text-muted mt-2">${refs.length ? '已带入该下单记录的运输编号，可按实际情况选择编号类型。' : '请填写真实运输编号；测试确认号 WWW0000000 不可用。'}</p>
            ${refs.length > 1 ? `<p class="small">本次下单编号：${refs.map(ref => esc(data.reference_types[ref.type])+ '：'+esc(ref.number)).join('；')}</p>` : ''}
            <div data-followup-error class="alert alert-danger" hidden style="white-space:pre-wrap;"></div>
            <hr><h5>物流状态</h5><p class="small text-muted">以下为接口返回的当前状态及运输日期，并非完整的逐节点轨迹。</p><div data-followup-tracking></div>
            <hr><h5>运输文件</h5><p class="small text-muted">文件按页提供图片链接；POD、账单等是否可用，取决于上传进度及 API 账号权限。账单须使用 PRO号查询。</p><div data-followup-documents></div>`;
        renderTracking(data.tracking); renderDocuments(data.documents);
        if (!dialog.open) dialog.showModal();
    }
    document.addEventListener('click', async event => {
        const button = event.target.closest('.abf-followup-button');
        if (!button || button.disabled) return;
        button.disabled = true;
        try { await open(button.dataset.historyId || '', button.closest('dialog, .modal')); }
        catch (error) { alert(error.message); }
        finally { button.disabled = false; }
    });
    window.ABFFollowup = {button: (historyId = '', label = '物流跟踪 / 文件获取') => `<button type="button" class="btn btn-sm btn-outline-primary mt-2 abf-followup-button" data-history-id="${esc(historyId)}">${esc(label)}</button>`};
})();
