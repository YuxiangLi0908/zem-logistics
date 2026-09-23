// DOM logic checks; no real database or carrier calls.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = name => fs.readFileSync(path.resolve(__dirname, '../../static/js/' + name), 'utf8');
class Element {
    constructor() { this.value = ''; this.innerHTML = ''; this.events = {}; this.hidden = false; this.style = {}; }
    addEventListener(name, fn) { this.events[name] = fn; }
    insertAdjacentHTML(_, text) { this.innerHTML += text; }
    querySelector() { return this.child ||= new Element(); }
    reset() {}
    click() { return this.events.click?.({}); }
}
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const helpers = {esc, date:v => v || '—', labels:{success:'success'}};
vm.createContext(helpers);
const shared = source('auto_quote.js');
vm.runInContext(shared.slice(shared.indexOf('    function rows(value)'), shared.indexOf('    async function detail()')), helpers);
const nodes = {}, el = id => nodes[id] ||= new Element();
let ready, calls = [], taskFilters = [], shown, cleared = 0, fail = false, resolver;
const quote = i => ({price:100+i,rank:i+1,platform:'kakas',carrier:'<unsafe>',service:'STD',platform_complete:true,currency_assumed:false});
const rates = Array.from({length:200}, (_,i)=>({carrierName:'Carrier'+i,totalPrice:200-i}));
const report = {total:2,page:1,pages:2,addresses:[{key:'route',label:'Main <Street>'}],address:'route',
    profiles:[{id:1,code:'AQ000001'}],profile:1,leads:['2'],lead:'2',currencies:['USD'],currency:'USD',
    chart:[{id:1,batch_id:1,time:'2026-09-20T10:00:00Z',prices:Array.from({length:10},(_,i)=>quote(i))},
           {id:2,batch_id:2,time:'2026-09-21T10:00:00Z',prices:[]},
           {id:3,batch_id:3,time:'2026-09-22T10:00:00Z',prices:[quote(0)]}],
    rows:[{id:1,batch_id:1,operator:'<operator>',origin:'NJ',profile_code:'AQ000001',address:{address:'Main',city:'Rahway',state:'NJ',zipcode:'07065'},status:'success',result:{results:{kakas:{rates}}}}]};
const ui = {...helpers, refresh(){}, filterTasks(start,end){taskFilters.push([start,end]);}, clearSelection(){cleared++;}, async showBatch(id){shown=id;},
    async get(args) { calls.push({...args}); if (fail) throw Error('Failed'); if (args.q === 'slow') return new Promise(r=>{resolver=r;}); return structuredClone(report); }};
const context = {console,URLSearchParams,Date,window:{AutoQuoteUI:ui},document:{getElementById:el,addEventListener:(_,fn)=>{ready=fn;}}};
const tick = () => new Promise(setImmediate);
async function search(q) { el('destination-query').value=q; el('destination-search').events.submit({preventDefault(){}}); await tick(); }
async function main() {
    vm.createContext(context); vm.runInContext(source('auto_quote_destination.js'),context); ready();
    await search('Rahway');
    assert.equal(el('auto-task-list').hidden,false);
    assert.equal(calls[0].q,'Rahway');
    assert.equal((el('destination-chart').innerHTML.match(/<g data-rank=/g)||[]).length,10);
    assert(!el('destination-chart').innerHTML.includes('NaN'));
    assert(el('destination-chart').innerHTML.includes('&lt;unsafe&gt;'));
    const curve=el('destination-chart').innerHTML.match(/<path d="([^"]*)"/)[1];
    assert.equal((curve.match(/M/g)||[]).length,2); assert(!curve.includes('L'));
    const table=el('destination-rows').innerHTML;
    assert(table.includes('&lt;operator&gt;'));
    assert.equal((table.split('<details')[0].match(/Carrier/g)||[]).length,10);
    assert(table.includes('Carrier0 ')); // Highest quote remains available when expanded.
    el('destination-legend').events.change({target:{dataset:{rank:'0'},checked:false}});
    assert.equal(el('destination-chart').child.style.display,'none');
    await el('destination-rows').events.click({target:{closest:()=>({dataset:{batch:'1'}})}});
    assert.equal(shown,'1');
    el('destination-next').click(); await tick(); assert.equal(calls.at(-1).page,2);
    await search('slow'); await search('new'); resolver({...report,total:999}); await tick();
    assert(!el('destination-message').textContent.includes('999'));
    fail=true; await search('error'); assert.equal(el('destination-message').textContent,'Failed');
    assert.equal(el('destination-rows').innerHTML,'');
    el('destination-reset').click(); assert.equal(el('destination-history').hidden,true); assert.equal(el('auto-task-list').hidden,false);
    assert(cleared>=4);
    el('destination-start').value='2026-09-01'; el('destination-end').value='2026-09-23';
    await search('');
    assert.deepEqual(taskFilters.at(-1), ['2026-09-01','2026-09-23']);
    assert.equal(el('destination-history').hidden,true);

    // The new auto-creation page has no task table or detail DOM.
    const createNodes = Object.fromEntries(['quote-mode','auto-group','auto-group-count','auto-ui-message','auto-upload','auto-address-file','auto-import-message','auto-address-list'].map(id=>[id,new Element()]));
    createNodes['quote-mode'].value='auto'; createNodes['auto-group'].value='NJ';
    let creationReady;
    const creation = {console,URL,URLSearchParams,FormData,crypto:require('node:crypto').webcrypto,location:{origin:'https://example.invalid',search:''},window:{},setInterval(){},
        document:{getElementById:id=>createNodes[id]||null,querySelector:()=>({value:'csrf'}),addEventListener:(_,fn)=>{creationReady=fn;}},
        fetch:async(_url,args)=>({ok:true,json:async()=>args?.method==='POST'?{success:true,batch:{id:12,total:5}}:{success:true,page:1,groups:{NJ:5}}})};
    vm.createContext(creation); vm.runInContext(shared,creation); creationReady(); await tick();
    await creation.window.AutoQuoteUI.start({items:[]});
    assert(createNodes['auto-ui-message'].textContent.includes('#12'));
    assert(createNodes['auto-group-count'].textContent.includes('5'));
    console.log('Destination UI checks passed: top 10, gaps, escaping, full expandable quotes, paging, stale responses, errors/reset, standalone task creation.');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
