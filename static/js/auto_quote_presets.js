document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById(id);
    if (!$('quote-cargo-source')) return;
    let profiles = [], applied = null, generation = 0;
    const message = (text, error = false) => {
        $('quote-profile-message').textContent = text;
        $('quote-profile-message').className = 'small mt-2 ' + (error ? 'text-danger' : 'text-primary');
    };
    const selected = () => profiles.find(p => String(p.id) === $('quote-cargo-profile').value);
    async function load() {
        const run = ++generation;
        $('quote-cargo-profile').disabled = true;
        message('正在读取已有货物组…');
        try {
            const response = await fetch('/post_nsop/?step=auto_quote_data&kind=analysis_options', {cache:'no-store'});
            if (response.redirected) throw new Error('登录已过期，请刷新页面重新登录');
            const data = await response.json();
            if (!response.ok || !data.success) throw new Error(data.message || '读取货物组失败');
            if (run !== generation) return;
            profiles = data.profiles;
            const select = $('quote-cargo-profile'), previous = select.value;
            select.replaceChildren(new Option('请选择货物明细，选中后自动填入', ''));
            profiles.forEach(p => {
                const cargo = p.configuration.items.map(i => `长宽高 ${i.length}×${i.width}×${i.height} in / 单板重量 ${i.weight} lb / ${i.palletCount}板`).join('；');
                select.add(new Option(`${p.code} · ${p.origin} · ${cargo} · 申报价值 $${p.configuration.declaredValue}`, String(p.id)));
            });
            select.value = profiles.some(p => String(p.id) === previous) ? previous : '';
            message(profiles.length ? '选择一条货物明细即可自动填入。' : '暂无可用组。首次手动输入并提交自动询价后，会自动生成AQ编号，下次即可选择。');
        } catch (error) {
            if (run === generation) message(error.message || '读取货物组失败，请重试或手动输入。', true);
        } finally {
            if (run === generation) $('quote-cargo-profile').disabled = false;
        }
    }
    $('quote-cargo-source').addEventListener('change', () => {
        const saved = $('quote-cargo-auto').checked;
        $('quote-preset-controls').hidden = !saved;
        if (saved) load();
        else { ++generation; applied = null; message('已切换手动输入，保留当前已填内容。'); }
    });
    $('quote-cargo-profile').addEventListener('change', () => {
        const profile = selected();
        if (!profile) return;
        try {
            window.AutoQuoteForm.applyProfile(profile.configuration);
            applied = profile.code;
            message(`已填入 ${applied}。可在下方查看或修改货物明细。`);
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
