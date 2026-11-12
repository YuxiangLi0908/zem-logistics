/* Compact P1 warnings; retain the complete provider response in collapsed details. */
(() => {
    const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({
        '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
    }[char]));

    function brief(message) {
        const text = String(message || '').trim();
        if (/not an active user|invaliduserexception/i.test(text)) return '承运商账号未激活或无效，请联系 P1 核实';
        if (/unauthorized|\b(?:http|status(?:\s+code)?|code)\s*[:=]?\s*401\b/i.test(text)) return '承运商认证未通过，请联系 P1 核实授权';
        if (/forbidden|\b(?:http|status(?:\s+code)?|code)\s*[:=]?\s*403\b/i.test(text)) return '无承运商询价权限，请联系 P1 核实';
        if (/internal\s*server\s*error|\b(?:http|status(?:\s+code)?|code)\s*[:=]?\s*500\b/i.test(text)) return '承运商服务异常（500），请稍后重试';
        if (/too many requests|\b(?:http|status(?:\s+code)?|code)\s*[:=]?\s*429\b/i.test(text)) return '请求过于频繁，请稍后重试';
        if (/time[ -]?out|timed out/i.test(text)) return '承运商响应超时，请稍后重试';
        // Unknown errors keep their actual reason; only technical markup/stack frames are hidden.
        const plain = text.replace(/<[^>]*>/g, ' ').split(/\s+at\s+[\w.]+\(/i)[0].replace(/\s+/g, ' ').trim();
        return plain ? plain.slice(0, 140) + (plain.length > 140 ? '…' : '') : '未返回可用报价，请展开查看完整报错';
    }

    function warning(state) {
        if (!state?.warning) return '';
        const lines = String(state.warning).split('；').map(text => text.trim()).filter(Boolean);
        const items = [...new Set(lines.map(line => {
            const divider = line.indexOf('：');
            const carrier = divider >= 0 ? line.slice(0, divider) : '';
            const reason = brief(divider >= 0 ? line.slice(divider + 1) : line);
            return `<li>${carrier ? `<strong>${esc(carrier)}</strong>：` : ''}${esc(reason)}</li>`;
        }))];
        return `<div class="alert alert-warning small mt-3 mb-0" style="overflow-wrap:anywhere">` +
            `<div class="fw-bold mb-1">询价提示（不影响以上已返回的报价）</div>` +
            `<ul class="mb-2 ps-3">${items.join('')}</ul>` +
            `<details><summary style="cursor:pointer">查看完整报错</summary>` +
            `<pre class="small mt-2 mb-0" style="white-space:pre-wrap;max-height:180px;overflow:auto">${esc(state.warning)}</pre>` +
            `</details></div>`;
    }

    window.P1QuoteUI = {warning};
    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('[data-p1-quote-warning]').forEach(element => {
            const full = element.querySelector('pre')?.textContent;
            if (full) element.innerHTML = warning({warning:full});
        });
    });
})();
