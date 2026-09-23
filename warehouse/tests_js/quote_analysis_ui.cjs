// Deterministic UI logic smoke test. Does not replace a browser/layout test.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class Element {
    constructor() { this.value = ''; this.checked = false; this.hidden = false; this.disabled = false; this.events = {}; this.children = {}; }
    set innerHTML(html) {
        this.html = html;
        this.children = {};
        for (const match of html.matchAll(/data-line="(\d+)" checked/g)) this.children[match[1]] = {checked:true};
    }
    get innerHTML() { return this.html || ''; }
    addEventListener(name, fn) { this.events[name] = fn; }
    querySelector(selector) { return this.children[selector.match(/data-line="(\d+)"/)[1]]; }
}
const nodes = {};
const el = id => nodes['qa-' + id] ||= new Element();
let ready, calls = [], failNext = false;
const configuration = {origin:{City:'Newark',State:'NJ',PostCode:'07101',DetailAddress:'1 Street'},declaredValue:'100',quoteType:1,needLiftgate:false,destinationType:1,items:[]};
const product = {platform:'kakas',carrier:'Carrier <unsafe>',service:'STD',volatility_pct:10,routes:1,available_routes:1,coverage_pct:66.67,min_samples:3,comparable_routes:true};
const report = {
    profile:{id:1,code:'AQ000001',origin:'NJ',configuration},
    filters:{lead:2,currency:'USD',timezone:'UTC'}, addresses:[{id:'route1',label:'Newark'}], carriers:[{id:'carrier1',label:'Carrier STD'}],lead_days:[2,3],
    summary:{routes:1,daily_samples:3,series:1,most_volatile:product,most_stable:product,common_routes:1,balanced_series:1,winner_changes:0},
    diagnostics:{}, rankings:[product], rows:[{...product,address:'Newark',route:'route1',samples:2,expected:3,latest:null,previous:100,minimum:100,maximum:110,mean:105}],
    page:1,pages:1,total:1,minimum_history:[],market_index:[{date:'2026-09-18',value:100},{date:'2026-09-19',value:110}],
    chart:[{...product,points:[{date:'2026-09-18',price:100,batch_id:1},{date:'2026-09-19',price:null,batch_id:2},{date:'2026-09-20',price:110,batch_id:3}]}],
};
const context = {console, URLSearchParams, Date, Intl, location:{search:'?profile=1'},
    document:{getElementById:id => nodes[id] ||= new Element(), addEventListener:(_, fn) => {ready = fn;}},
    fetch:async url => {
        const params = new URL(url, 'https://example.invalid').searchParams;
        calls.push(params);
        if (failNext) {failNext = false; return {ok:false,json:async()=>({success:false,message:'Test failure'})};}
        const result = params.get('kind') === 'analysis_options' ? {profiles:[report.profile],timezone:'UTC'} : report;
        return {ok:true,json:async()=>({success:true,...result})};
    },
};
const tick = () => new Promise(setImmediate);
report.route_rows = [{route:'route1',address:'Newark',latest:100,previous:90,latest_change:10,latest_change_pct:11.11,
    minimum:90,maximum:100,mean:95,volatility_pct:null,samples:2,expected:3,coverage_pct:66.67,
    winners:['Carrier A'],services:[report.rows[0], {...report.rows[0],carrier:'Carrier B'}]}];
async function main() {
    vm.createContext(context);
    vm.runInContext(fs.readFileSync(path.resolve(__dirname, '../../static/js/auto_quote_analysis.js'), 'utf8'), context);
    ready(); await tick();
    assert.equal(el('content').hidden, false);
    assert.equal((el('series').innerHTML.match(/data-route=/g) || []).length, 1);
    assert(el('series').innerHTML.includes('Carrier B'));
    assert(el('series').innerHTML.includes('<details>'));
    assert.equal(calls.find(p => p.get('kind') === 'analysis').get('group_by'), 'address');
    assert(el('chart').innerHTML.includes('<svg'));
    assert(!el('chart').innerHTML.includes('NaN'));
    assert(el('ranking').innerHTML.includes('Carrier &lt;unsafe&gt;'));
    assert(!el('ranking').innerHTML.includes('Carrier <unsafe>'));
    assert(el('export').href.includes('kind=analysis_export'));
    assert(el('export').href.includes('lead=2'));
    assert.equal(el('lead').value, '2');
    el('address').value = 'route1'; el('address').events.change(); await tick();
    const svg = el('chart').innerHTML;
    assert(svg.includes('batch=1'));
    assert(svg.includes('batch=3'));
    assert(!svg.includes('batch=2'));
    const curve = svg.match(/<path d="([^"]*)"/)[1];
    assert.equal((curve.match(/M/g) || []).length, 2, 'missing price must break the line');
    assert(!curve.includes('L'), 'must not join points across a missing quote');
    el('legend').children['0'].checked = false; el('legend').events.change();
    assert(!el('chart').innerHTML.includes('<svg'));
    failNext = true; el('form').events.submit({preventDefault(){}}); await tick();
    assert.equal(el('error').hidden, false);
    assert.equal(el('error').textContent, 'Test failure');
    assert.equal(el('content').hidden, true);
    assert.equal(el('export').hidden, true);
    assert.equal(el('submit').disabled, false);
    assert(calls.some(p => p.get('address') === 'route1'));
    console.log('Analysis UI smoke checks passed: initialization, SVG index/trend, gaps, legend, escaping, filters, export context, error state.');
}
main().catch(error => {console.error(error); process.exitCode = 1;});
