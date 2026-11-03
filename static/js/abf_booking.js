/* Manual ABF BOL + pickup. Booking source always comes from saved server history. */
(() => {
    if (window.ABFBooking) return;
    const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    let dialog, current, busy = false;
    const api = async (step, fields = {}) => {
        const data = new FormData();
        data.set('step', step);
        data.set('csrfmiddlewaretoken', document.querySelector('[name=csrfmiddlewaretoken]').value);
        Object.entries(fields).forEach(([key, value]) => data.set(key, value));
        const response = await fetch('/post_nsop/', {method:'POST', body:data, credentials:'same-origin'});
        if (response.redirected) throw new Error('登录状态已失效，请刷新后核实下单记录。');
        let result;
        try { result = await response.json(); }
        catch (_) { throw new Error('服务器未返回有效响应，请重新打开本窗口查看保存的下单状态。'); }
        if (!response.ok || !result.success) throw new Error(result.message || `请求失败 HTTP ${response.status}`);
        return result;
    };
    function ensureDialog() {
        if (dialog) return;
        dialog = document.createElement('dialog');
        dialog.id = 'abf-booking-dialog';
        dialog.style.cssText = 'width:960px;max-width:94vw;max-height:90vh;margin:auto;padding:24px;border:1px solid #ddd;border-radius:12px;overflow:auto;';
        document.body.appendChild(dialog);
        dialog.addEventListener('click', event => {
            if (event.target.closest('[data-abf-close]') && !busy) dialog.close();
        });
        dialog.addEventListener('cancel', event => { if (busy) event.preventDefault(); });
        dialog.addEventListener('submit', submit);
    }
    function showBooking(booking) {
        const area = dialog.querySelector('[data-abf-result]');
        const form = dialog.querySelector('form');
        const status = booking?.status;
        if (!status) { area.innerHTML = ''; form.hidden = false; return; }
        const done = ['success','test_success'].includes(status);
        const uncertain = ['submitting','unknown'].includes(status);
        form.hidden = done || uncertain;
        area.innerHTML = `<div class="alert ${done ? 'alert-success' : 'alert-warning'}" style="white-space:normal;overflow-wrap:anywhere;">
            <strong>${status === 'success' ? 'ABF 下单成功' : status === 'test_success' ? '测试通过（未正式预约提货）' : uncertain ? '请核实本次下单状态，勿重复提交' : 'ABF 拒绝本次下单，可按原因修改'}</strong>
            <div>${esc(booking.message || (uncertain ? '请求已发出或正在处理。请向 ABF 核实，重新打开窗口可查看最新状态。' : ''))}</div>
            ${booking.confirmation ? `<div>提货确认号：<strong>${esc(booking.confirmation)}</strong></div>` : ''}
            ${booking.pro_number ? `<div>PRO号：${esc(booking.pro_number)}</div>` : ''}
            ${booking.bol_number ? `<div>BOL编号：${esc(booking.bol_number)}</div>` : ''}
            ${[['document','下载 BOL'],['label_document','下载货物标签'],['pro_label_document','下载 PRO 标签']].map(([key,label]) => booking[key] ? `<a class="btn btn-sm btn-outline-primary me-2" href="${esc(booking[key])}" target="_blank" rel="noopener noreferrer">${label}</a>` : '').join('')}
        </div>`;
    }
    async function open(historyId, container) {
        if (busy) return;
        ensureDialog();
        // 保持在原 Bootstrap 弹框内，兼容其键盘焦点限制；原生 dialog
        // 仍在浏览器顶层显示，不受外层滚动区裁切。
        (container || document.body).appendChild(dialog);
        const data = await api('abf_booking_preview', {history_id:historyId});
        current = data;
        const source = data.form, quote = data.quote;
        const input = (name, label, value = '', type = 'text', required = true, extra = '') => `<label style="display:block;min-width:0;">${label}${required ? ' *' : ''}<input class="form-control" style="width:100%;box-sizing:border-box;" name="${name}" type="${type}" value="${esc(value)}" ${required ? 'required' : ''} ${extra}></label>`;
        dialog.innerHTML = `<div class="d-flex justify-content-between"><h4>ABF 下单 · 生成 BOL 并预约提货</h4><button type="button" data-abf-close class="btn btn-light" aria-label="关闭">×</button></div>
            <div class="alert ${data.mode === 'live' ? 'alert-info' : 'alert-warning'}">${data.mode === 'live' ? '正式下单：提交后会向 ABF 预约提货。' : '当前为测试模式：仅验证请求，不作为正式提货预约。'} 发货方为我们，运费预付。</div>
            <div class="mb-3">报价编号：<strong>${esc(quote.quoteId)}</strong>　报价：<strong>$${esc(quote.TotalQuote)}</strong><br>
            提货日期：${esc(source.pickupDate)}（沿用询价日期）<br>
            发货：${esc([source.originWarehouse,source.originDetailAddress,source.originCity,source.originState,source.originPostCode].join(' '))}<br>
            收货：${esc([source.destinationWarehouse,source.destinationDetailAddress,source.destinationCity,source.destinationState,source.destinationPostCode].join(' '))}</div>
            <details class="mb-3"><summary>本次询价货物（下单沿用此快照）</summary><div class="table-responsive"><table class="table table-sm"><thead><tr><th>描述</th><th>长×宽×高(in)</th><th>单板磅重</th><th>板数</th></tr></thead><tbody>${source.items.map(item => `<tr><td>${esc(item.description)}</td><td>${esc([item.length,item.width,item.height].join('×'))}</td><td>${esc(item.weight)}</td><td>${esc(item.palletCount)}</td></tr>`).join('')}</tbody></table></div></details>
            <div data-abf-result></div><form>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;">
            ${input('RequesterName','申请人姓名',data.requester.name)}${input('RequesterPhone','申请人电话')}${input('RequesterEmail','申请人邮箱',data.requester.email,'email')}
            ${input('ShipName','发货公司','ZEM LOGISTICS INC')}${input('ShipContact','发货联系人')}${input('ShipPhone','发货电话')}${input('ShipEmail','发货邮箱','','email',false)}
            ${input('ConsName','收货公司',source.destinationWarehouse)}${input('ConsContact','收货联系人')}${input('ConsPhone','收货电话')}${input('ConsEmail','收货邮箱','','email',false)}
            ${input('OT','开门时间（当地）','','time',true,'step="1800"')}${input('AT','货好时间（当地）','','time',true,'step="1800"')}${input('CT','关门时间（当地）','','time',true,'step="1800"')}
            ${input('Bol','BOL编号','ZEMABF'+historyId,'text',true,'maxlength="20"')}
            </div><p class="small text-muted mt-2">时间按发货仓当地时间填写，需为整点或半点。尺寸、地址、日期或附加服务有变动，请重新询价后再下单。</p>
            <label style="display:block;">装运备注<textarea name="Instructions" class="form-control" maxlength="370" rows="2"></textarea></label>
            <label style="display:block;margin:16px 0;"><input type="checkbox" required> 我已核对报价、收发信息和提货安排，确认${data.mode === 'live' ? '提交正式提货预约' : '提交测试请求'}。</label>
            <div data-abf-error class="alert alert-danger" hidden style="white-space:pre-wrap;"></div>
            <button type="submit" class="btn btn-success">${data.mode === 'live' ? '确认下单并预约提货' : '提交测试下单'}</button></form>`;
        showBooking(data.booking);
        if (!dialog.open) dialog.showModal();
    }
    async function submit(event) {
        event.preventDefault();
        if (busy) return;
        const form = event.target;
        if (!form.reportValidity()) return;
        if (!confirm(current.mode === 'live' ? '确认向 ABF 正式预约提货并生成 BOL？' : '确认提交测试请求？')) return;
        const button = form.querySelector('[type=submit]'), error = form.querySelector('[data-abf-error]');
        busy = true; button.disabled = true; button.textContent = '正在提交，请勿重复操作…'; error.hidden = true;
        try {
            const fields = Object.fromEntries(new FormData(form));
            const result = await api('abf_booking_submit', {history_id:current.history_id, booking_fields:JSON.stringify(fields), confirmed:'yes'});
            showBooking(result.booking);
            if (['success','test_success','unknown','submitting'].includes(result.booking.status)) {
                document.querySelectorAll('.abf-book-button').forEach(btn => { if (btn.dataset.historyId === String(current.history_id)) btn.textContent = '查看下单结果'; });
            }
        } catch (failure) {
            error.textContent = failure.message; error.hidden = false;
            // Re-read durable state before allowing another submission after a lost response.
            try { const saved = await api('abf_booking_preview', {history_id:current.history_id}); showBooking(saved.booking); }
            catch (_) { form.hidden = true; dialog.querySelector('[data-abf-result]').textContent = '无法确认下单状态，请稍后重新打开窗口核实，勿重复提交。'; }
        } finally { busy = false; button.disabled = false; button.textContent = current.mode === 'live' ? '确认下单并预约提货' : '提交测试下单'; }
    }
    document.addEventListener('click', async event => {
        const button = event.target.closest('.abf-book-button');
        if (!button || button.disabled) return;
        button.disabled = true;
        try { await open(button.dataset.historyId, button.closest('.modal')); }
        catch (error) { alert(error.message); }
        finally { button.disabled = false; }
    });
    window.ABFBooking = {button: historyId => `<button type="button" class="btn btn-success btn-sm mt-3 abf-book-button" data-history-id="${esc(historyId)}">下单（生成BOL并预约提货）</button>`};
})();
