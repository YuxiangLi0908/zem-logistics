/* Shared route-analysis Excel data and download flow for analysis and task history. */
(() => {
    const platformName = value => ({maersk:'Maersk', kakas:'卡卡省', abf:'ABF', unknown:'未知平台'}[value] || value);
    const num = value => value == null ? '—' : Number(value).toFixed(2);
    const percent = value => value == null ? '—' : Number(value).toFixed(2) + '%';
    const movement = value => value == null ? '—' : (value > 0 ? '+' : '') + Number(value).toFixed(2);
    const text = value => String(value ?? '').normalize('NFKC').trim().toLowerCase();
    const lines = (...values) => values.map(value => String(value ?? '').replace(/\s+/g, ' ').trim()).filter(Boolean).join('\n');
    function platforms(routes) {
        const keys = [...new Set(routes.flatMap(route => (route.services || []).map(service => service.platform || 'unknown')))].sort();
        return keys.length ? keys : ['maersk','kakas'];
    }
    function carrierPrices(services) {
        const price = service => service.latest == null ? Infinity : Number(service.latest);
        const cheapest = predicate => {
            let best = null;
            services.forEach(service => {
                if (Number.isFinite(price(service)) && predicate(service) && (best === null || price(service) < price(best))) best = service;
            });
            return best === null ? null : best.latest;
        };
        return [`最低价：${num(cheapest(() => true))}`,
            `TForce：${num(cheapest(service => /\bt\s*force\b/i.test(text(service.carrier))))}`,
            `Estes：${num(cheapest(service => /\bestes\b/i.test(text(service.carrier))))}`].join('\n');
    }
    function routeSheet(routes, keys = platforms(routes)) {
        const header = ['收货地址','距离（英里）','最新最低价 / 提供方','上次最低价','较上次涨跌','区间最低 / 最高','日最低价均值','日最低价CV','询价次数',
            ...keys.map(key => `${platformName(key)} 承运商明细`)];
        return [header, ...routes.map(route => [
            lines(route.address), num(route.distance_miles), lines(num(route.latest), route.winner_carrier || '本次无有效报价'),
            lines(num(route.previous), route.previous_date), lines(movement(route.latest_change), movement(route.latest_change_pct) + '%'),
            `${num(route.minimum)} / ${num(route.maximum)}`, num(route.mean), percent(route.volatility_pct), String(route.expected ?? 0),
            ...keys.map(key => carrierPrices((route.services || []).filter(service => (service.platform || 'unknown') === key)))
        ])];
    }
    function defaultDates(timezone, latestPickup, now = new Date()) {
        const parts = new Intl.DateTimeFormat('en-CA', {timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(now);
        const part = name => parts.find(value => value.type === name).value;
        const today = `${part('year')}-${part('month')}-${part('day')}`;
        const end = /^\d{4}-\d{2}-\d{2}$/.test(latestPickup || '') && latestPickup > today ? latestPickup : today;
        const start = new Date(end + 'T00:00:00Z'); start.setUTCDate(start.getUTCDate() - 29);
        return {start:start.toISOString().slice(0,10), end};
    }
    async function responseError(response) {
        const raw = await response.text();
        let message = '';
        try {
            const data = JSON.parse(raw);
            message = data.message || data.error || data.detail || '';
            if (typeof message !== 'string') message = JSON.stringify(message);
        } catch (_) {
            if (/^\s*</.test(raw)) {
                const title = raw.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
                message = title ? title[1].replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim() : '';
            } else message = raw.replace(/\s+/g, ' ').trim();
        }
        const hints = {400:'请求被拒绝，可能是导出数据格式错误或超过服务器请求大小限制',403:'没有导出权限或登录验证已失效，请刷新页面后重试',
            404:'导出接口不存在，请确认服务器已部署导出功能',413:'导出数据超过服务器上传大小限制，请缩小筛选范围或调整服务器限制',
            500:'服务器生成 Excel 时发生异常，请查看服务器错误日志',502:'服务器网关异常，导出服务可能未正常响应',503:'导出服务暂时不可用',
            504:'服务器生成文件超时，请缩小筛选范围后重试'};
        return `HTTP ${response.status}${response.statusText ? ' ' + response.statusText : ''}：${message.slice(0,400) || hints[response.status] || '服务器返回异常响应'}${message && hints[response.status] ? '；'+hints[response.status] : ''}`;
    }
    async function requestExcel(sheets, title, csrf) {
        const form = new FormData();
        form.set('step','auto_quote_table_export'); form.set('title',title);
        form.set('sheets_file',new Blob([JSON.stringify(sheets)], {type:'application/json'}),'analysis.json');
        form.set('csrfmiddlewaretoken',csrf);
        const response = await fetch('/post_nsop/', {method:'POST',body:form});
        if (response.redirected) throw new Error('登录已过期，请刷新后重新登录');
        if (!response.ok) throw new Error(await responseError(response));
        if (!response.headers.get('Content-Type')?.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
            throw new Error('接口未返回 Excel 文件。' + await responseError(response));
        }
        return response.blob();
    }
    function download(blob, filename) {
        const url = URL.createObjectURL(blob), link = document.createElement('a');
        link.href = url; link.download = filename;
        document.body.appendChild(link); link.click(); link.remove();
        setTimeout(() => URL.revokeObjectURL(url),1000);
    }
    window.AutoQuoteExport = {platforms, routeSheet, defaultDates, requestExcel, download};
})();
