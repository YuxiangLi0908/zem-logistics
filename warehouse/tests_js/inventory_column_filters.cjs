const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const source = fs.readFileSync(path.resolve(__dirname,
    '../templates/post_port/inventory/01_inventory_management_main.html'), 'utf8');
const header = source.match(/<tr id="warehouse-table-filter"[\s\S]*?<\/tr>/)[0];
const inputs = [...header.matchAll(/<th\b[^>]*>([\s\S]*?)<\/th>/g)]
    .flatMap((match, cellIndex) => match[1].includes('<input')
        ? [{value: '', closest: () => ({cellIndex})}] : []);
function row(mark, fba, ref, weight, pcs, cbm, pallets) {
    const values = ['', 'Customer', 'JXLU7818200', 'HOLD', '2026-10-06',
        'LA-91730', 'UPS', 'public', mark, fba, ref, weight, pcs, cbm, pallets];
    return {style: {}, querySelectorAll: () => values.map(textContent => ({textContent: String(textContent)}))};
}
const rows = [row('OSW26090413336', 'FBA-A', 'REF-A', 10.5, 3, 1.25, 2),
    row('X002WCPA1J', 'FBA-B', 'REF-B', 20, 4, 2, 1)];
const elements = Object.fromEntries(['total-weight', 'total-pcs', 'total-cbm', 'total-pallets']
    .map(id => [id, {textContent: ''}]));
elements['warehouse-table-filter'] = {querySelectorAll: () => inputs};
const context = {document: {querySelectorAll: () => rows, getElementById: id => elements[id]}};
vm.createContext(context);
vm.runInContext(source.slice(source.indexOf('    function filterTable()'), source.indexOf('    window.onload')), context);
function search(filters, visible) {
    inputs.forEach(input => { input.value = filters[input.closest().cellIndex] || ''; });
    context.filterTable();
    assert.deepEqual(rows.map(r => r.style.display !== 'none'), visible);
}
search({8: ' osw2609 '}, [true, false]);
assert.equal(elements['total-weight'].textContent, '10.50');
assert.equal(elements['total-pcs'].textContent, 3);
assert.equal(elements['total-cbm'].textContent, '1.25');
assert.equal(elements['total-pallets'].textContent, 2);
search({2: 'JXLU7818200', 3: 'hold', 8: 'x002'}, [false, true]);
search({9: 'fba-b'}, [false, true]);
search({10: 'ref-a'}, [true, false]);
search({8: 'public'}, [false, false]); // Must not search the delivery-type column.
assert.equal(elements['total-weight'].textContent, '0.00');
search({8: 'osw', 9: 'fba-b'}, [false, false]);
search({}, [true, true]);
assert.equal(elements['total-weight'].textContent, '30.50');
assert.equal(elements['total-pcs'].textContent, 7);
assert.equal(elements['total-cbm'].textContent, '3.25');
assert.equal(elements['total-pallets'].textContent, 3);
console.log('Inventory filtering passed: marks, FBA, REF, combined filters, normalization, clearing and totals.');
