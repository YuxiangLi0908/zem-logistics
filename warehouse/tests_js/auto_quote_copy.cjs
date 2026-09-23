const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');
const source = fs.readFileSync(path.resolve(__dirname, '../../static/js/auto_quote.js'), 'utf8');
const context = {
    esc: value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    date: value => value || '—', labels:{success:'Success'},
};
vm.createContext(context);
vm.runInContext(source.slice(source.indexOf('    function rows(value)'), source.indexOf('    async function detail()')), context);
const rates = Array.from({length:200}, (_, i) => ({carrierName:`Carrier${200-i}`, totalPrice:200-i}));
const carrier = {data:{rates}};
const original = JSON.stringify(carrier);
const folded = context.prices(carrier, {limit:10,key:'kakas-5'});
const visible = folded.split('<details')[0];
assert.equal((visible.match(/<div>/g) || []).length, 10);
assert(visible.includes('Carrier1 '));
assert(visible.includes('Carrier10 '));
assert(!visible.includes('Carrier11 '));
assert(folded.includes('Carrier200 '));
assert(!folded.includes('data-id="kakas-5" open'));
assert(context.prices(carrier, {limit:10,key:'kakas-5',open:true}).includes('data-id="kakas-5" open'));
assert(!context.prices({rates:rates.slice(0,10)}, {limit:10}).includes('<details'));
const item = {address:{city:'=unsafe',state:'NJ',zipcode:'07001',address:'<b>Address</b>',distance_miles:'5'},status:'success',started_at:'2026-09-22',result:{results:{kakas:carrier,maersk:{quotes:[{TotalQuote:20,DisplayService:'M'}]},abf:{rates:[{price:30,carrierName:'A'}]}}}};
const copied = context.copyTable({rows:[item, {...item,address:{...item.address,city:'Second'}}]});
assert.equal(copied.count, 2);
assert.equal((copied.html.match(/Carrier/g) || []).length, 400);
assert.equal((copied.text.match(/Carrier/g) || []).length, 400);
assert(copied.html.indexOf('Carrier1 ') < copied.html.indexOf('Carrier200 '));
assert(!copied.html.includes('<details'));
assert(copied.html.includes('&lt;b&gt;Address&lt;/b&gt;'));
assert(copied.text.includes("'=unsafe"));
assert(copied.html.includes('$20.00') && copied.html.includes('$30.00'));
assert.equal(JSON.stringify(carrier), original);
console.log('Quote folding/copy checks passed: 200 rates, lowest 10, expansion, full HTML/TSV, all platforms, escaping, unchanged source.');
