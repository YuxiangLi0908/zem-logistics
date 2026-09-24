const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const template = fs.readFileSync(path.resolve(__dirname,'../templates/post_port/new_sop/leader_check/multi_carrier_quote.html'),'utf8');
const nodes = {};
for (const id of ['quote_origin_warehouse','quote_type','quote_car_type','quote_declared_value','quote_commodity_unit','quote_pallet_type','quote_destination_type','quote_liftgate','quote_pickup_date']) nodes[id] = {value:'',checked:false};
for (const [id, values] of Object.entries({quote_type:['1','2'],quote_car_type:['','1','2'],quote_commodity_unit:['11','2'],quote_pallet_type:['1','2'],quote_destination_type:['1','2','3']})) nodes[id].options = values.map(value=>({value}));
nodes.quote_pickup_date.value = '2026-10-10';
let cargo = [], chosenOrigin;
const context = {window:{},document:{getElementById:id=>nodes[id]},
    warehouses:[{warehouse:'NJ renamed',city:'Avenel',state:'NJ',postCode:'07001',detailAddress:'27 Engelhard Ave'}],
    setOrigin: name=>{chosenOrigin=name;},addItem:row=>cargo.push(row),itemsBody:{set innerHTML(_){cargo=[];}}};
vm.createContext(context);
vm.runInContext(template.slice(template.indexOf('    function setQuoteType()'), template.indexOf('    function payload()')), context);
const c = {origin:{City:'avenel',State:'nj',PostCode:'07001',DetailAddress:'27 engelhard ave'},
    quoteType:1,carType:null,declaredValue:'1200',commodityUnit:11,palletType:1,destinationType:1,needLiftgate:true,
    items:[{description:'Pallet',pieces:'1',length:'48',width:'40',height:'72',weight:'1100',palletCount:'1'},
           {description:'Box',pieces:'2',length:'24',width:'20',height:'36',weight:'50',palletCount:'2'}]};
context.window.AutoQuoteForm.applyProfile(c);
assert.equal(cargo.length,2);assert.equal(cargo[0].weight,'1100');
assert.equal(chosenOrigin,'NJ renamed');assert.equal(nodes.quote_declared_value.value,'1200');
assert.equal(nodes.quote_liftgate.checked,true);assert.equal(nodes.quote_pickup_date.value,'2026-10-10');
assert.equal(nodes.quote_car_type.disabled,true);
context.window.AutoQuoteForm.applyProfile({...c,quoteType:2,carType:2});
assert.equal(nodes.quote_car_type.value,'2');assert.equal(nodes.quote_car_type.disabled,false);
const before=JSON.stringify(cargo);
assert.throws(()=>context.window.AutoQuoteForm.applyProfile({...c,origin:{...c.origin,City:'Unknown'}}));
assert.equal(JSON.stringify(cargo),before);
assert.throws(()=>context.window.AutoQuoteForm.applyProfile({...c,commodityUnit:999}));
assert.equal(JSON.stringify(cargo),before);
console.log('Saved cargo checks passed: multi-row fill, full conditions, renamed origin, unchanged pickup date, FTL/LTL, invalid preset leaves form intact.');
