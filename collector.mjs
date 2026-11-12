import { chromium } from 'playwright';
import fs from 'node:fs/promises';
const hotels=JSON.parse(await fs.readFile('hotels.json','utf8'));
await fs.mkdir('data',{recursive:true});
const browser=await chromium.launch({headless:true,args:['--no-sandbox']});
const names=['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'];
const collectScript=({names})=>{
 const out={};const start=new Date();start.setDate(1);start.setHours(0,0,0,0);
 const end=new Date(start.getFullYear(),start.getMonth()+12,1);
 for(const table of [...document.querySelectorAll('table')].filter(t=>/dom\.|seg\.|ter\./i.test(t.innerText))){
  let prev=table.previousElementSibling,head='';
  for(let j=0;j<5&&prev;j++,prev=prev.previousElementSibling)head+=' '+prev.innerText;
  if(!head.trim())head=table.parentElement?.parentElement?.innerText.slice(0,100)||'';
  const match=head.toLowerCase().match(/(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s+de\s+(\d{4})/);
  if(!match)continue;
  const month=names.indexOf(match[1]),year=Number(match[2]);
  for(const td of table.querySelectorAll('td')){
   const parts=td.innerText.trim().split(/\s+/),day=Number(parts[0]);
   if(!day||day>31||parts.length<2)continue;
   const date=new Date(year,month,day);
   if(date<start||date>=end)continue;
   const raw=parts[1];
   if(!/^(?:[\d.,]+K?|—)$/i.test(raw))continue;
   const price=raw==='—'?null:/K$/i.test(raw)?Math.round(parseFloat(raw.replace(',','.'))*1000):Number(raw.replace(/\./g,'').replace(',','.'));
   if(price!==null&&(!Number.isFinite(price)||price<=0))continue;
   const key=[year,String(month+1).padStart(2,'0'),String(day).padStart(2,'0')].join('-');
   out[key]={price,approx:/K$/i.test(raw)};
  }
 }
 return out;
};
for(const hotel of hotels){
 const slug=new URL(hotel.url).pathname.split('/').pop().replace(/\.html$/,'');
 const page=await browser.newPage({locale:'pt-BR',timezoneId:'America/Sao_Paulo',viewport:{width:1440,height:1000}});
 const all={};
 try{
  await page.goto(hotel.url,{waitUntil:'domcontentloaded',timeout:45000});
  await page.waitForTimeout(3500);
  for(let i=0;i<13;i++){
   const found=await page.evaluate(collectScript,{names});
   Object.assign(all,found);
   console.log(hotel.name,'passo',i,'datas',Object.keys(all).length);
   const next=page.getByRole('button',{name:/mês seguinte/i}).first();
   if(!await next.count()||!await next.isEnabled())break;
   await next.click({timeout:5000});
   await page.waitForTimeout(1000);
  }
  if(!Object.keys(all).length){console.error('Sem tarifas verificadas:',hotel.name);continue;}
  const path='data/'+slug+'.json';
  await fs.writeFile(path,JSON.stringify({name:hotel.name,url:hotel.url,updatedAt:new Date().toISOString(),rates:all},null,2));
  console.log('SALVO',path,Object.keys(all).length);
 }catch(e){console.error('Falha',hotel.name,String(e).slice(0,500))}
 finally{await page.close()}
}
await browser.close();
