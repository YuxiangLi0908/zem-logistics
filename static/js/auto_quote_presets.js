/* Saved AQ groups. Each member starts its own ordinary address-quote batch. */
document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById(id), ui = window.AutoQuoteUI;
    if (!$('quote-cargo-source') || !ui) return;
    const esc = ui.esc;
    let profiles = [], groups = [], selectedIds = new Set(), editing = null, loading = false, saving = false;
    const active = () => $('quote-cargo-auto').checked;
    const selectedGroup = () => groups.find(group => String(group.id) === $('quote-cargo-group').value);
    const profileById = id => profiles.find(profile => String(profile.id) === String(id));
    const message = (text, error = false) => {
        $('quote-profile-message').textContent = text;
        $('quote-profile-message').className = 'small mt-2 ' + (error ? 'text-danger' : 'text-primary');
    };
    const cargo = profile => profile.configuration.items.map(item =>
        `${item.length}×${item.width}×${item.height} in / 单板 ${item.weight} lb / ${item.palletCount}板 / 每板${item.pieces}件`
    ).join('；');
    const label = profile => `${profile.code} · ${profile.origin} · ${cargo(profile)} · 申报价值 $${profile.configuration.declaredValue}`;
    function updatePlan() {
        const group = selectedGroup(), count = ui.addressCount();
        $('quote-cargo-plan').textContent = group ?
            `本次将按组内顺序生成 ${group.profile_ids.length} 个任务 × ${count} 个地址，共 ${group.profile_ids.length * count} 条地址询价。` : '';
    }
    function controls() {
        const busy = loading || saving;
        $('quote-cargo-group').disabled = busy || !active();
        $('quote-cargo-group').required = active();
        $('quote-cargo-new').disabled = busy;
        $('quote-cargo-edit').disabled = busy || !selectedGroup();
        $('quote-cargo-delete').disabled = busy || !selectedGroup();
        $('quote-cargo-editor-fields').disabled = busy;
    }
    function preview() {
        const group = selectedGroup();
        $('quote-cargo-preview').innerHTML = group ?
            '<table class="table table-sm table-bordered mb-0"><thead><tr><th>序号</th><th>AQ编号</th><th>发货仓</th><th>货物明细</th><th>申报价值</th><th>服务条件</th></tr></thead><tbody>' +
            group.profile_ids.map((id, index) => {
                const profile = profileById(id);
                if (!profile) return `<tr><td>${index + 1}</td><td colspan="5">该 AQ 已不可用，请编辑货物明细组</td></tr>`;
                const c = profile.configuration;
                const services = [(c.quoteType === 2 ? `FTL（车型 ${c.carType}）` : 'LTL'),
                    {1:'商业地址',2:'住宅地址',3:'装卸平台'}[c.destinationType] || '', c.needLiftgate ? '需要尾板' : '无需尾板'].join(' / ');
                return `<tr><td>${index + 1}</td><td>${esc(profile.code)}</td><td>${esc(profile.origin)}</td><td style="min-width:260px;white-space:normal">${esc(cargo(profile))}</td><td>$${esc(c.declaredValue)}</td><td>${esc(services)}</td></tr>`;
            }).join('') + '</tbody></table>' : '';
        updatePlan(); controls();
    }
    function receive(data, preferred) {
        profiles = data.profiles;
        groups = data.cargo_groups;
        const value = String(preferred ?? $('quote-cargo-group').value);
        $('quote-cargo-group').replaceChildren(new Option('请选择货物明细组', ''));
        groups.forEach(group => $('quote-cargo-group').add(new Option(`${group.name}（${group.profile_ids.length}条）`, String(group.id))));
        $('quote-cargo-group').value = groups.some(group => String(group.id) === value) ? value : '';
        preview();
    }
    async function post(step, values) {
        const body = new FormData();
        body.append('step', step);
        body.append('csrfmiddlewaretoken', $('standaloneQuoteForm').querySelector('[name=csrfmiddlewaretoken]').value);
        Object.entries(values).forEach(([key, value]) => body.append(key, value));
        const response = await fetch('/post_nsop/', {method:'POST', body});
        if (response.redirected) throw new Error('登录已过期，请刷新后重新登录');
        const data = await response.json();
        if (!response.ok || !data.success) throw new Error(data.message || '操作失败');
        return data;
    }
    async function load() {
        if (loading) return;
        loading = true; controls(); message('正在读取货物明细组…');
        try {
            receive(await ui.get({kind:'cargo_groups'}));
            message(groups.length ? '选择货物明细组后，点击“开始自动询价”即可逐条提交。' :
                (profiles.length ? '暂无货物明细组，请点击“新建组”，从已有 AQ 中多选保存。' : '暂无 AQ 货物明细，请先使用“手动输入单次货物”提交一次自动询价，再创建组。'));
        } catch (error) { message(error.message || '读取货物明细组失败，请重试。', true); }
        finally { loading = false; controls(); }
    }
    function renderChoices() {
        const query = $('quote-cargo-search').value.trim().toLowerCase();
        const matching = profiles.filter(profile => label(profile).toLowerCase().includes(query));
        $('quote-cargo-options').innerHTML = matching.map(profile =>
            `<label class="d-flex gap-2 align-items-start border-bottom py-2"><input type="checkbox" data-profile="${profile.id}" ${selectedIds.has(String(profile.id)) ? 'checked' : ''}><span style="white-space:normal">${esc(label(profile))}</span></label>`
        ).join('') || '<p class="small text-muted mb-0">没有匹配的 AQ 货物明细</p>';
        $('quote-cargo-selected').textContent = `已选择 ${selectedIds.size} 条：` + [...selectedIds].map(id => profileById(id)?.code).filter(Boolean).join(' → ');
    }
    function edit(group) {
        editing = group?.id || null;
        selectedIds = new Set((group?.profile_ids || []).map(String));
        $('quote-cargo-name').value = group?.name || '';
        $('quote-cargo-search').value = '';
        $('quote-cargo-editor').hidden = false;
        renderChoices();
        $('quote-cargo-name').focus();
        message('勾选需要的 AQ 并保存。组内每条分别询价，按勾选顺序执行。');
    }
    function mode() {
        $('quote-preset-controls').hidden = !active();
        window.AutoQuoteForm.setGroupedMode(active());
        if (!active()) { $('quote-cargo-editor').hidden = true; message('手动输入的多行货物作为同一票货物提交一次自动询价。'); }
        else load();
        controls();
    }
    $('quote-cargo-source').addEventListener('change', mode);
    $('quote-cargo-group').addEventListener('change', () => { $('quote-cargo-editor').hidden = true; preview(); message('已选择货物明细组，各条条件见上表。'); });
    $('quote-cargo-new').addEventListener('click', () => edit(null));
    $('quote-cargo-edit').addEventListener('click', () => edit(selectedGroup()));
    $('quote-cargo-cancel').addEventListener('click', () => { $('quote-cargo-editor').hidden = true; message('已取消编辑。'); });
    $('quote-cargo-search').addEventListener('input', renderChoices);
    $('quote-cargo-options').addEventListener('change', event => {
        const input = event.target.closest('input[data-profile]');
        if (!input) return;
        if (input.checked) selectedIds.add(input.dataset.profile);
        else selectedIds.delete(input.dataset.profile);
        renderChoices();
    });
    $('quote-cargo-save').addEventListener('click', async () => {
        const name = $('quote-cargo-name').value.trim();
        if (!name || !selectedIds.size || selectedIds.size > 50) { message('请填写组名称，并选择1至50条 AQ 货物明细。', true); return; }
        saving = true; controls(); message('正在保存货物明细组…');
        try {
            const data = await post('auto_quote_cargo_group_save', {cargo_group:editing || '', name, profile_ids:JSON.stringify([...selectedIds])});
            receive(data, data.cargo_group_id);
            $('quote-cargo-editor').hidden = true;
            message('货物明细组已保存，可以开始自动询价。');
        } catch (error) { message(error.message, true); }
        finally { saving = false; controls(); }
    });
    $('quote-cargo-delete').addEventListener('click', async () => {
        const group = selectedGroup();
        if (!group || !confirm(`确定删除货物明细组「${group.name}」吗？已有询价任务和结果会保留。`)) return;
        saving = true; controls();
        try {
            receive(await post('auto_quote_cargo_group_delete', {cargo_group:group.id}), '');
            $('quote-cargo-editor').hidden = true;
            message('货物明细组已删除，原有任务和结果保留。');
        } catch (error) { message(error.message, true); }
        finally { saving = false; controls(); }
    });
    window.AutoQuoteCargoGroups = {
        isActive:active, updatePlan,
        async submit() {
            if (loading || saving) throw new Error('正在读取或保存货物明细组，请稍后再提交');
            if (!$('quote-cargo-editor').hidden) throw new Error('请先保存或取消货物明细组的编辑，再开始询价');
            const group = selectedGroup();
            if (!group) throw new Error('请选择货物明细组');
            await ui.startGroup(group.id, $('quote_pickup_date').value, group.updated_at);
        },
    };
    mode();
});
