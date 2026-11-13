const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');
const read = filename => fs.readFileSync(path.resolve(__dirname, '../../static/js/' + filename), 'utf8');
const routes = [{route:'r1', address:'Newark 07101',distance_miles:0,latest:50,winner_carrier:'Supplier',
    previous:40,previous_date:'2026-11-03',latest_change:10,latest_change_pct:25,minimum:40,maximum:50,mean:45,volatility_pct:null,expected:2,
    services:[{platform:'maersk',carrier:'TForce',latest:100},{platform:'maersk',carrier:'Estes',latest:110},
        {platform:'maersk',carrier:'Other',latest:50},{platform:'kakas',carrier:'TForce',latest:80},
        {platform:'kakas',carrier:'Estes',latest:90},{platform:'kakas',carrier:'ABF Freight',latest:145},
        {platform:'kakas',carrier:'abf Freight System',latest:130},{platform:'abf',carrier:'ABF Freight',latest:120}]}];
const context = {window:{},Date,Intl,FormData,Blob,console,setTimeout,clearTimeout,setInterval,clearInterval,Map};
vm.createContext(context);
vm.runInContext(read('auto_quote_export.js'),context);
const shared = context.window.AutoQuoteExport;
const sheet = shared.routeSheet(routes);
assert.equal(sheet[0][9],'ABF 承运商明细');
assert.equal(sheet[0][10],'卡卡省 承运商明细');
assert.equal(sheet[0][11],'Maersk 承运商明细');
assert.equal(sheet[1][1],'0.00');
assert.equal(sheet[1][2],'50.00\nSupplier');
assert.equal(sheet[1][3],'40.00\n2026-11-03');
assert.equal(sheet[1][4],'+10.00\n+25.00%');
assert.equal(sheet[1][9],'最低价：120.00\nTForce：—\nEstes：—');
assert.equal(sheet[1][10],'最低价：80.00\nTForce：80.00\nEstes：90.00\nABF：130.00');
assert.equal(sheet[1][11],'最低价：50.00\nTForce：100.00\nEstes：110.00');
const withoutABF = {...routes[0],services:routes[0].services.filter(service => service.platform !== 'kakas' || !/\babf\b/i.test(service.carrier))};
assert(shared.routeSheet([withoutABF])[1][10].endsWith('ABF：—'), 'The standalone ABF platform must not fill the Kakas ABF price');
const scacABF = {...withoutABF,services:[...withoutABF.services,{platform:'kakas',carrier:'Account carrier',carrier_code:'abfs',latest:0}]};
assert(shared.routeSheet([scacABF])[1][10].endsWith('ABF：0.00'), 'ABFS SCAC and valid zero prices must be supported');
assert.equal(shared.defaultDates('America/New_York', '2026-11-10', new Date('2026-11-04T01:00Z')).start,'2026-10-12');
assert.equal(shared.defaultDates('America/New_York', null, new Date('2026-11-04T01:00Z')).end,'2026-11-03');

let sheets = [], downloads = [], requests = [], failAnalysis = false;
shared.requestExcel = async (value,title,csrf) => { sheets.push(JSON.parse(JSON.stringify(value))); assert.equal(title,'收货地址价格分析'); assert.equal(csrf,'test-csrf'); return new Blob(); };
shared.download = (blob,name) => downloads.push(name);
const report = {profile:{code:'AQ000001'},route_rows:routes};
const tableRow = {cells:[{},{}],querySelector:()=>({dataset:{route:'r1'}})};
const status = {textContent:'',setAttribute(){}};
const button = {id:'qa-export-routes',dataset:{id:'5',profile:'1'},disabled:false,innerHTML:'Export',setAttribute(){},removeAttribute(){},after(){}};
const nodes = {error:{hidden:true},form:{querySelector:()=>({value:'test-csrf'})},series:{rows:[tableRow],closest:()=>({})}};
Object.assign(context, {$:id=>nodes[id],routeReport:report,document:{getElementById:()=>status,querySelectorAll:()=>[button]},
    exportCellText:()=>{throw new Error('Route export should use analysis data, not nested detail tables');},
    csrf:'test-csrf',message:()=>{},get:async args=>{
        requests.push(args);
        if(args.kind==='analysis_options')return {timezone:'UTC',latest_pickup_date:'2099-01-31'};
        if(failAnalysis)throw Error('Analysis unavailable');
        return report;
    }});
vm.runInContext('let exportingBatch = null;',context);
const analysisSource=read('auto_quote_analysis.js');
vm.runInContext(analysisSource.slice(analysisSource.indexOf('    async function exportTable('),analysisSource.indexOf("    $('export-distance').addEventListener")),context);
const tasksSource=read('auto_quote.js');
vm.runInContext(tasksSource.slice(tasksSource.indexOf('    async function exportTaskAnalysis('),tasksSource.indexOf("    $('auto-batches')?.addEventListener('click'")),context);

async function main(){
    await context.exportTable(button,'series','收货地址价格分析');
    await context.exportTaskAnalysis(button);
    assert.deepEqual(sheets[0],sheets[1], 'task action and analysis-page export must produce identical Excel cells');
    const analysis=requests.find(args=>args.kind==='analysis');
    assert.equal(analysis.profile,'1');
    assert.equal(analysis.start,'2099-01-02');
    assert.equal(analysis.end,'2099-01-31');
    assert.equal(analysis.group_by,'address');
    assert.equal(analysis.all_routes,'1');
    assert.equal(analysis.include_partial,'1');
    assert(downloads[1].includes('AQ000001'));
    assert.equal(button.disabled,false);
    failAnalysis=true;
    await assert.rejects(()=>context.exportTaskAnalysis(button),/Analysis unavailable/);
    assert.equal(downloads.length,2);
    assert.equal(button.disabled,false);
    failAnalysis=false;
    await context.exportTaskAnalysis(button);
    assert.equal(downloads.length,3, 'failed export must release the in-flight guard');
    console.log('Route Excel checks passed: platform prices, dates, equal task/page sheets, full dataset, failure recovery.');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
