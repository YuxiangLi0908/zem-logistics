const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const read = name => fs.readFileSync(path.resolve(__dirname, '../../static/js/' + name), 'utf8');
class Element {
    constructor() { this.value=''; this.checked=false; this.disabled=false; this.hidden=false; this.innerHTML=''; this.events={}; this.options=[]; }
    addEventListener(name, callback) { this.events[name]=callback; }
    replaceChildren(...options) { this.options=options; }
    add(option) { this.options.push(option); }
    querySelector() { return {value:'csrf'}; }
    focus() {}
}
const nodes={}, el=id=>nodes[id] ||= new Element();
el('quote-cargo-auto').checked=true;
el('quote-cargo-editor').hidden=true;
el('quote_pickup_date').value='2026-12-01';
const profile=id=>({id,code:'AQ'+String(id).padStart(6,'0'),origin:'NJ <unsafe>',configuration:{
    items:[{length:48,width:40,height:70+id,weight:1100,palletCount:id,pieces:1}],declaredValue:2500,quoteType:1,destinationType:1,needLiftgate:false}});
let state={profiles:[profile(1),profile(2),profile(3)],cargo_groups:[{id:5,name:'Daily',profile_ids:[2,1],updated_at:'v1'}]};
let ready, posted=[], starts=[], grouped=[], fail=false;
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ui={esc,addressCount:()=>100,get:async args=>{assert.equal(args.kind,'cargo_groups');return structuredClone(state);},
    startGroup:async(...args)=>starts.push(args)};
const context={console,FormData,Set,Option:function(text,value){this.text=text;this.value=value;},confirm:()=>true,
    document:{getElementById:el,addEventListener:(_,fn)=>{ready=fn;}},
    window:{AutoQuoteUI:ui,AutoQuoteForm:{setGroupedMode:value=>grouped.push(value)}},
    fetch:async(_url,options)=>{
        const values=Object.fromEntries(options.body.entries());posted.push(values);
        if(fail)return {ok:false,json:async()=>({success:false,message:'Save failed'})};
        if(values.step==='auto_quote_cargo_group_save')state.cargo_groups=[{id:5,name:values.name,profile_ids:JSON.parse(values.profile_ids).map(Number),updated_at:'v2'}];
        if(values.step==='auto_quote_cargo_group_delete')state.cargo_groups=[];
        return {ok:true,json:async()=>({success:true,...structuredClone(state),cargo_group_id:5})};
    }};
const tick=()=>new Promise(setImmediate);
const choose=id=>({target:{closest:()=>({dataset:{profile:String(id)},checked:true})}});
async function main(){
    vm.createContext(context);vm.runInContext(read('auto_quote_presets.js'),context);ready();await tick();
    assert.deepEqual(grouped,[true]);
    el('quote-cargo-group').value='5';el('quote-cargo-group').events.change();
    assert(el('quote-cargo-preview').innerHTML.includes('AQ000002'));
    assert(el('quote-cargo-preview').innerHTML.includes('NJ &lt;unsafe&gt;'));
    assert(el('quote-cargo-plan').textContent.includes('200'));
    await context.window.AutoQuoteCargoGroups.submit();
    assert.deepEqual(starts,[[5,'2026-12-01','v1']]);
    el('quote-cargo-edit').events.click();
    await assert.rejects(context.window.AutoQuoteCargoGroups.submit(),/保存/);
    el('quote-cargo-options').events.change({target:{closest:()=>({dataset:{profile:'2'},checked:false})}});
    el('quote-cargo-search').value='AQ000003';el('quote-cargo-search').events.input();
    el('quote-cargo-options').events.change(choose(3));
    el('quote-cargo-name').value='Updated';await el('quote-cargo-save').events.click();
    assert.deepEqual(JSON.parse(posted.at(-1).profile_ids),['1','3']); // Unchecked 2 is never sent; hidden checked 1 is retained.
    assert.equal(el('quote-cargo-editor').hidden,true);
    assert.equal(el('quote-cargo-group').value,'5');
    await context.window.AutoQuoteCargoGroups.submit();assert.deepEqual(starts.at(-1),[5,'2026-12-01','v2']);
    el('quote-cargo-auto').checked=false;el('quote-cargo-source').events.change();
    assert.equal(grouped.at(-1),false);assert.equal(el('quote-cargo-group').disabled,true);
    el('quote-cargo-auto').checked=true;el('quote-cargo-source').events.change();await tick();
    el('quote-cargo-edit').events.click();fail=true;await el('quote-cargo-save').events.click();
    assert.equal(el('quote-profile-message').textContent,'Save failed');
    assert.equal(el('quote-cargo-editor').hidden,false);
    fail=false;el('quote-cargo-cancel').events.click();await el('quote-cargo-delete').events.click();
    assert.equal(el('quote-cargo-preview').innerHTML,'');

    // Actual form logic must ignore empty single-shipment fields when submitting a saved group.
    const template=fs.readFileSync(path.resolve(__dirname,'../templates/post_port/new_sop/leader_check/multi_carrier_quote.html'),'utf8');
    let click, groupSubmits=0, singles=0;
    const formContext={window:{AutoQuoteCargoGroups:{isActive:()=>true,submit:async()=>groupSubmits++},AutoQuoteUI:{start:async()=>singles++}},
        mode:{value:'auto'},form:{reportValidity:()=>true},errorBox:{style:{}},
        submitButton:{innerHTML:'Start',addEventListener:(_,fn)=>{click=fn;}},payload:()=>{throw Error('Group must not read the single-shipment payload');}};
    vm.createContext(formContext);
    const start=template.indexOf("    submitButton.addEventListener('click'");
    vm.runInContext(template.slice(start,template.indexOf('    const tomorrow',start)),formContext);
    await click();assert.equal(groupSubmits,1);assert.equal(singles,0);
    assert.equal(formContext.submitButton.disabled,false);
    console.log('Cargo group UI checks passed: saved order, previews, full selection across filters, unchecked IDs excluded, group-only submission, manual mode, error recovery and deletion.');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
