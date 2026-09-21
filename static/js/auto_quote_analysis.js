document.addEventListener('DOMContentLoaded', () => {
    const $ = id => document.getElementById('qa-' + id);
    if (!$('form')) return;
    const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const platform = value => ({maersk:'Maersk', kakas:'卡卡省', abf:'ABF'}[value] || value);
    const percent = value => value === null || value === undefined ? '—' : Number(value).toFixed(2) + '%';
    const num = value => value === null || value === undefined ? '—' : Number(value).toFixed(2);
    const movement = value => value === null || value === undefined ? '—' : `<span class="${value > 0 ? 'qa-up' : value < 0 ? 'qa-down' : ''}">${value > 0 ? '+' : ''}${Number(value).toFixed(2)}</span>`;
    const product = row => row ? `${platform(row.platform)} / ${row.carrier} / ${row.service}` : '样本不足或没有可比较线路';
    let page = 1, chartLines = [], generation = 0;
    const colors = ['#2563eb','#c2410c','#059669','#9333ea','#be185d','#0e7490','#a16207','#475569'];
    async function api(args) {
        const query = new URLSearchParams({step:'auto_quote_data', ...args});
        const response = await fetch('/post_nsop/?' + query, {cache:'no-store'});
        if (response.redirected) throw new Error('登录已过期，请刷新后重新登录');
        let result;
        try { result = await response.json(); } catch (_) { throw new Error('无法读取分析数据，请确认已部署新代码并完成迁移'); }
        if (!response.ok || !result.success) throw new Error(result.message || '分析失败');
        return result;
    }
    function options(id, values, label, selected) {
        $(id).innerHTML = `<option value="">${esc(label)}</option>` + values.map(v => `<option value="${esc(v.id)}">${esc(v.label)}</option>`).join('');
        if (values.some(v => String(v.id) === String(selected))) $(id).value = selected;
    }
    function showConfiguration(configuration) {
        const c = configuration;
        $('profile-info').innerHTML = `<div>发货地址：${esc(Object.values(c.origin).join(', '))}</div><div>申报价值：$${esc(c.declaredValue)} · ${c.quoteType === 1 ? 'LTL' : 'FTL'} · 尾板：${c.needLiftgate ? '需要' : '不需要'} · 收货地址类型：${esc({1:'商业地址',2:'住宅地址',3:'装卸平台'}[c.destinationType])}</div>` +
            c.items.map(item => `<div>${esc(item.description)}：${esc(item.palletCount)}板，${esc(item.pieces)}件/板，${esc(item.length)} × ${esc(item.width)} × ${esc(item.height)} in，单板 ${esc(item.weight)} lb</div>`).join('');
    }
    async function analyze() {
        if (!$('profile').value) return;
        const run = ++generation;
        $('submit').disabled = true;
        $('error').hidden = true;
        $('export').hidden = true;
        const args = {kind:'analysis', page};
        for (const key of ['profile','group','start','end','address','platform','carrier','lead','currency']) args[key] = $(key).value;
        args.include_partial = $('partial').checked ? '1' : '0';
        try {
            const data = await api(args);
            if (run !== generation) return;
            page = data.page;
            $('export').href = '/post_nsop/?' + new URLSearchParams({step:'auto_quote_data', ...args, kind:'analysis_export', lead:data.filters.lead ?? ''});
            $('export').hidden = false;
            options('address', data.addresses, '全部地址 · 综合指数', args.address);
            options('carrier', data.carriers, '全部承运商服务', args.carrier);
            $('lead').innerHTML = '<option value="">自动选择样本最多的提前天数</option><option value="all">全部（混合提前天数）</option>' + data.lead_days.map(day => `<option value="${day}">${day}天后取件</option>`).join('');
            $('lead').value = data.filters.lead === null ? '' : String(data.filters.lead);
            showConfiguration(data.profile.configuration);
            $('count').textContent = `${data.profile.code} · ${data.summary.routes}条线路 · ${data.summary.daily_samples}个地址采样日 · ${data.summary.series}条价格序列 · ${data.filters.currency}`;
            render(data);
            $('content').hidden = false;
        } catch (error) {
            if (run !== generation) return;
            $('error').hidden = false; $('error').textContent = error.message; $('content').hidden = true;
        } finally { if (run === generation) $('submit').disabled = false; }
    }
    function render(data) {
        const s = data.summary, d = data.diagnostics;
        $('kpis').innerHTML = [
            ['波动最大（平均CV）', percent(s.most_volatile?.volatility_pct), product(s.most_volatile) + (s.most_volatile_ties > 1 ? `（共${s.most_volatile_ties}个服务并列，见下表）` : '')],
            ['波动最小（平均CV）', percent(s.most_stable?.volatility_pct), product(s.most_stable) + (s.most_stable_ties > 1 ? `（共${s.most_stable_ties}个服务并列，见下表）` : '')],
            ['较上次涨幅最大', percent(s.largest_increase?.latest_change_pct), s.largest_increase ? product(s.largest_increase) + ' · ' + s.largest_increase.address : '暂无上涨的可比较报价'],
            ['较上次跌幅最大', percent(s.largest_decrease?.latest_change_pct), s.largest_decrease ? product(s.largest_decrease) + ' · ' + s.largest_decrease.address : '暂无下跌的可比较报价'],
        ].map(([title, value, note]) => `<div class="qa-kpi"><span>${esc(title)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join('');
        const notes = [`按 ${data.filters.timezone} 划分采样日。`, `最低价提供方在相邻有效采样日变化 ${s.winner_changes} 次。`];
        if (data.filters.lead === 'all') notes.push('当前混合了不同取件提前天数，变化可能受取件时间影响。');
        else if (data.filters.lead !== null) notes.push(`当前仅比较提前 ${data.filters.lead} 个日历日的报价。`);
        if (d.partial_excluded) notes.push(`已排除 ${d.partial_excluded} 条未完整平台报价。`);
        if (d.assumed_currency) notes.push(`${d.assumed_currency} 条报价未声明币种，按美国国内报价USD处理。`);
        if (d.invalid_identity) notes.push(`${d.invalid_identity} 条报价因承运商标识不完整未参与。`);
        if (d.unindexed) notes.push(`有 ${d.unindexed} 条历史询价尚未归档，请联系管理员完成历史归档后再解读结果。`);
        if (d.duplicates_collapsed) notes.push(`同次同服务多条报价取最低，归并 ${d.duplicates_collapsed} 条重复服务报价。`);
        $('quality').textContent = notes.join(' ');
        $('ranking-note').textContent = s.common_routes ? `使用各承运商/服务共有的 ${s.common_routes} 条线路，每条线路至少3个共同报价日，线路等权平均CV；数值越大波动越大。请结合样本量和覆盖率判断，不能把缺报价当作稳定。` : '当前没有足够的共同线路和共同报价日。下表仅显示各自样本的参考值，不判定哪个承运商最稳定。';
        const max = Math.max(1, ...data.rankings.map(r => r.volatility_pct));
        $('ranking').innerHTML = data.rankings.map((r, index) => `<tr><td>${r.comparable_routes ? data.rankings.findIndex(v => v.volatility_pct === r.volatility_pct) + 1 : '参考'}</td><td>${esc(platform(r.platform))}</td><td>${esc(r.carrier)} / ${esc(r.service)}</td><td><span class="qa-bar" style="width:${r.volatility_pct / max * 100}px"></span> ${percent(r.volatility_pct)}</td><td>${r.routes} / 可用${r.available_routes}</td><td>${percent(r.coverage_pct)}</td><td>${r.min_samples}天</td></tr>`).join('') || '<tr><td colspan="7">至少需要同一线路、同一服务3个有效报价日才能计算波动排名。</td></tr>';
        $('series').innerHTML = data.rows.map(r => `<tr><td><button type="button" class="btn btn-link btn-sm" data-route="${esc(r.route)}">${esc(r.address)}</button></td><td>${esc(product(r))}</td><td>${num(r.latest)}<div class="qa-note">${esc(r.latest_date || '')}</div></td><td>${num(r.previous)}<div class="qa-note">${esc(r.previous_date || '')}</div></td><td>${movement(r.latest_change)}</td><td>${movement(r.latest_change_pct)}</td><td>${movement(r.period_change_pct)}</td><td>${num(r.minimum)} / ${num(r.maximum)}</td><td>${num(r.mean)}</td><td>${percent(r.volatility_pct)}</td><td>${percent(r.range_pct)}</td><td>${percent(r.max_adjacent_move_pct)}</td><td>${r.samples} / ${r.expected}</td><td>${percent(r.coverage_pct)}</td></tr>`).join('') || '<tr><td colspan="14">当前筛选范围内没有可分析报价。请检查日期、提前天数、币种及是否已有完成的询价任务。</td></tr>';
        $('page').textContent = `${data.page} / ${data.pages} 页，共${data.total}条序列`;
        $('prev').disabled = data.page <= 1; $('next').disabled = data.page >= data.pages;
        const addressSelected = Boolean($('address').value);
        $('min-section').hidden = !addressSelected;
        $('minima').innerHTML = data.minimum_history.map(r => `<tr><td>${esc(r.date)}</td><td>${num(r.price)} ${esc(data.filters.currency)}</td><td>${esc(r.winners.join('；') || '无报价')}</td><td>${r.offers}</td><td>${r.winner_changed ? '已变化' : '—'}</td></tr>`).join('');
        $('chart-title').textContent = addressSelected ? '所选地址 · 各承运商服务价格走势' : '固定线路与服务 · 综合价格指数';
        $('chart-note').textContent = addressSelected ? `单位：${data.filters.currency}。取消勾选图例可隐藏曲线，悬停查看数值，点击数据点查看原询价任务。缺报价处断开。${data.chart_truncated ? '当前显示样本最多的8条曲线，可选择承运商查看其他服务。' : ''}` : `首日=100，使用全期间每天均有报价的 ${s.balanced_series} 条固定线路/服务等权计算。选择一个收货地址可查看实际价格曲线。`;
        chartLines = addressSelected ? data.chart.map((row, index) => ({name:product(row), color:colors[index % colors.length], points:row.points})) : data.market_index.length ? [{name:'固定样本价格指数', color:colors[0], points:data.market_index.map(p => ({date:p.date, price:p.value}))}] : [];
        $('legend').innerHTML = chartLines.map((line, index) => `<label style="color:${line.color}"><input type="checkbox" data-line="${index}" checked> ${esc(line.name)}</label>`).join('');
        drawChart();
    }
    function drawChart() {
        const lines = chartLines.filter((_, index) => $('legend').querySelector(`[data-line="${index}"]`)?.checked);
        const points = lines.flatMap(line => line.points).filter(p => p.price !== null);
        if (!points.length) { $('chart').innerHTML = '<p class="qa-note p-4">暂无可绘制的数据。综合指数至少需要2个采样日，且固定样本在每个展示日都有正数报价。</p>'; return; }
        const allDates = [...new Set(lines.flatMap(line => line.points.map(p => p.date)))].sort();
        const times = allDates.map(day => Date.parse(day + 'T00:00:00Z'));
        const t0 = Math.min(...times), t1 = Math.max(...times);
        const min = Math.min(...points.map(p => p.price)), max = Math.max(...points.map(p => p.price));
        const padding = Math.max((max - min) * .15, max * .03, 1), lo = Math.max(0, min - padding), hi = max + padding;
        const x = date => 80 + (t1 === t0 ? 430 : (Date.parse(date + 'T00:00:00Z') - t0) / (t1 - t0) * 860);
        const y = price => 290 - (price - lo) / (hi - lo) * 240;
        let svg = '<svg viewBox="0 0 1000 350" role="img" aria-label="价格随询价日期变化的折线图"><rect width="1000" height="350" fill="white"/>';
        for (let i = 0; i <= 4; i++) {
            const value = lo + (hi - lo) * i / 4, yy = y(value);
            svg += `<line x1="80" x2="940" y1="${yy}" y2="${yy}" stroke="#e2e8f0"/><text x="70" y="${yy+4}" text-anchor="end" fill="#64748b" font-size="12">${num(value)}</text>`;
        }
        allDates.filter((_, i) => i % Math.max(1, Math.ceil(allDates.length / 6)) === 0 || i === allDates.length - 1).forEach(day => {
            svg += `<text x="${x(day)}" y="320" text-anchor="middle" fill="#64748b" font-size="12">${esc(day)}</text>`;
        });
        for (const line of lines) {
            let path = '', connected = false;
            for (const point of line.points) {
                if (point.price === null) { connected = false; continue; }
                path += `${connected ? 'L' : 'M'}${x(point.date)},${y(point.price)} `; connected = true;
            }
            svg += `<path d="${path}" fill="none" stroke="${line.color}" stroke-width="2.5"/>`;
            for (const point of line.points.filter(p => p.price !== null)) {
                const dot = `<circle cx="${x(point.date)}" cy="${y(point.price)}" r="4" fill="${point.partial ? 'white' : line.color}" stroke="${line.color}" tabindex="0"><title>${esc(line.name)} · ${esc(point.date)} · ${num(point.price)}${point.partial ? '（部分报价）' : ''}</title></circle>`;
                svg += point.batch_id ? `<a href="/post_nsop/?step=auto_quote_history&amp;batch=${point.batch_id}">${dot}</a>` : dot;
            }
        }
        $('chart').innerHTML = svg + '</svg>';
    }
    $('legend').addEventListener('change', drawChart);
    $('form').addEventListener('submit', event => { event.preventDefault(); page = 1; analyze(); });
    for (const id of ['profile','group','start','end']) $(id).addEventListener('change', () => { $('address').value = ''; $('carrier').value = ''; $('lead').value = ''; });
    $('platform').addEventListener('change', () => { $('carrier').value = ''; });
    $('address').addEventListener('change', () => { page = 1; analyze(); });
    $('series').addEventListener('click', event => { const button = event.target.closest('[data-route]'); if (button) { $('address').value = button.dataset.route; page = 1; analyze(); } });
    $('prev').addEventListener('click', () => { page--; analyze(); });
    $('next').addEventListener('click', () => { page++; analyze(); });
    async function init() {
        try {
            const data = await api({kind:'analysis_options'});
            const parts = new Intl.DateTimeFormat('en-CA', {timeZone:data.timezone,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date());
            const part = name => parts.find(p => p.type === name).value;
            const end = `${part('year')}-${part('month')}-${part('day')}`;
            const start = new Date(end + 'T00:00:00Z'); start.setUTCDate(start.getUTCDate() - 29);
            $('end').value = end; $('start').value = start.toISOString().slice(0,10);
            options('profile', data.profiles.map(p => ({id:p.id,label:`${p.code} · ${p.origin} · 申报$${p.configuration.declaredValue}`})), '请选择比较编号', new URLSearchParams(location.search).get('profile'));
            if (!$('profile').value && data.profiles.length) $('profile').value = data.profiles[0].id;
            if (!data.profiles.length) { $('count').textContent = '暂无比较编号。请先完成自动询价，或完成历史数据归档。'; return; }
            await analyze();
        } catch (error) { $('error').hidden = false; $('error').textContent = error.message; }
    }
    init();
});
