document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById(id);
    if (!$('quote-cargo-source')) return;
    let profiles = [], applied = null, generation = 0;
    const message = (text, error = false) => {
        $('quote-profile-message').textContent = text;
        $('quote-profile-message').className = 'small mt-2 ' + (error ? 'text-danger' : 'text-primary');
    };
    const selected = () => profiles.find(p => String(p.id) === $('quote-cargo-profile').value);
    function preview() {
        const profile = selected();
        $('quote-apply-profile').disabled = !profile;
        const c = profile?.configuration;
        $('quote-profile-preview').textContent = c ?
            `${profile.code} · ${profile.origin} · 申报价值 $${c.declaredValue} · ${Number(c.quoteType) === 2 ? 'FTL' : 'LTL'} · ${c.needLiftgate ? '需要尾板' : '不需要尾板'}\n` +
            c.items.map(i => `${i.description}：${i.palletCount}板，${i.pieces}件/板，${i.length} × ${i.width} × ${i.height} in，单板 ${i.weight} lb`).join('\n') : '';
    }
    async function load() {
        const run = ++generation;
        $('quote-apply-profile').disabled = true;
        $('quote-reload-profiles').disabled = true;
        message('正在读取已有货物组…');
        try {
            const response = await fetch('/post_nsop/?step=auto_quote_data&kind=analysis_options', {cache:'no-store'});
            if (response.redirected) throw new Error('登录已过期，请刷新页面重新登录');
            const data = await response.json();
            if (!response.ok || !data.success) throw new Error(data.message || '读取货物组失败');
            if (run !== generation) return;
            profiles = data.profiles;
            const select = $('quote-cargo-profile'), previous = select.value;
            select.replaceChildren(new Option('请选择已有组', ''));
            profiles.forEach(p => select.add(new Option(`${p.code} · ${p.origin} · ${p.configuration.items.length}行货物 · 申报 $${p.configuration.declaredValue}`, String(p.id))));
            select.value = profiles.some(p => String(p.id) === previous) ? previous : '';
            preview();
            message(profiles.length ? '选择后先查看明细，点击“填入这组配置”应用。' : '暂无可用组。首次手动输入并提交自动询价后，会自动生成AQ编号，下次即可选择。');
        } catch (error) {
            if (run === generation) message(error.message || '读取货物组失败，请重试或手动输入。', true);
        } finally {
            if (run === generation) $('quote-reload-profiles').disabled = false;
        }
    }
    $('quote-cargo-source').addEventListener('change', () => {
        const saved = $('quote-cargo-source').value === 'saved';
        $('quote-preset-controls').hidden = !saved;
        if (saved) load();
        else { ++generation; applied = null; message('已切换手动输入，保留当前已填内容。'); }
    });
    $('quote-reload-profiles').addEventListener('click', load);
    $('quote-cargo-profile').addEventListener('change', preview);
    $('quote-apply-profile').addEventListener('click', () => {
        const profile = selected();
        if (!profile) return;
        try {
            window.AutoQuoteForm.applyProfile(profile.configuration);
            applied = profile.code;
            message(`已填入 ${applied}。取件日期和收货地址组保持不变；货物及条件可继续编辑，提交时系统会根据最终内容确定AQ编号。`);
        } catch (error) { message(error.message, true); }
    });
    const edited = event => {
        if (!applied || event.target.id === 'quote_pickup_date' || event.target.id?.startsWith('quote-cargo-')) return;
        message(`已基于 ${applied} 修改，提交时会按最终货物、申报价值及服务条件匹配或生成AQ编号。`);
    };
    $('standaloneQuoteForm').addEventListener('input', edited);
    $('standaloneQuoteForm').addEventListener('change', edited);
    $('addQuoteItem').addEventListener('click', edited);
    $('quote_items').addEventListener('click', event => { if (event.target.closest('.remove-item')) edited(event); });
});
