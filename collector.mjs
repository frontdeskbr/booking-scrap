import { chromium } from 'playwright';
import fs from 'node:fs/promises';
const hotels=JSON.parse(await fs.readFile('hotels.json','utf8'));
await fs.mkdir('data',{recursive:true});
const browser=await chromium.launch({headless:true,args:['--no-sandbox']});
const collectScript=()=>{
 const rates={};
 const start=new Date();start.setDate(1);start.setHours(0,0,0,0);
 const end=new Date(start.getFullYear(),start.getMonth()+12,1);
 const key=d=>[d.getFullYear(),String(d.getMonth()+1).padStart(2,'0'),String(d.getDate()).padStart(2,'0')].join('-');
 for(const el of document.querySelectorAll('table[role="grid"] [data-date]')){
  if(!el.getClientRects().length)continue;
  const date=el.getAttribute('data-date');
  if(!date||date<key(start)||date>=key(end))continue;
  const raw=(el.getAttribute('aria-describedby')||'').split(/\s+/).map(id=>document.getElementById(id)?.innerText||'').join(' ').trim();
  const text=raw.replace(/\s/g,'').replace(/^R\$/,'');
  const short=text.match(/^(\d+(?:[.,]\d+)?)K$/i);
  const numeric=/^\d+(?:\.\d{3})*(?:,\d+)?$/.test(text);
  const price=short?Number(short[1].replace(',','.'))*1000:numeric?Number(text.replace(/\./g,'').replace(',','.')):null;
  if(price===null&&text!=='—')continue;
  if(price!==null&&(!Number.isFinite(price)||price<=0))continue;
  rates[date]={price,approx:true,display:raw};
 }
 return rates;
};
for(const hotel of hotels){
 const slug=new URL(hotel.url).pathname.split('/').pop().replace(/\.html$/,'');
 const page=await browser.newPage({locale:'pt-BR',timezoneId:'America/Sao_Paulo',viewport:{width:1440,height:1000}});
 const all={};
 try{
  const target=new URL(hotel.url);target.searchParams.set('selected_currency','BRL');target.searchParams.set('group_adults','2');target.searchParams.set('no_rooms','1');
  const checkin=new Date();checkin.setDate(checkin.getDate()+7);const checkout=new Date(checkin);checkout.setDate(checkout.getDate()+2);target.searchParams.set('checkin',checkin.toISOString().slice(0,10));target.searchParams.set('checkout',checkout.toISOString().slice(0,10));
  await page.goto(target.href,{waitUntil:'domcontentloaded',timeout:45000});
  await page.waitForTimeout(3500);
  console.log('PAGE',page.url(),'TITLE',await page.title());
  console.log('BUTTONS',(await page.locator('button').allTextContents()).slice(0,35).map(x=>x.trim().slice(0,45)));
  console.log('TABLES',await page.locator('table').count(),'BODY',(await page.locator('body').innerText()).slice(0,700));
  for(const sel of ['button:has-text("Selecionar datas")','[data-testid="searchbox-dates-container"]','button:has-text("Visualizar preços")','[data-testid="date-display-field-start"]','button[aria-label*="data"]','[data-testid="searchbox-dates-checkin"]']){try{let el=page.locator(sel).filter({visible:true}).last();if(await el.count()){await el.click({timeout:1500});await page.waitForTimeout(1500);console.log('AFTER CLICK TABLES',await page.locator('table').count(),'NEXT',await page.getByRole('button',{name:/mês seguinte/i}).count());console.log('AFTER TEXT',(await page.locator('body').innerText()).slice(-3800));console.log('CALENDAR PARENT',(await page.locator('table').last().evaluate(el=>el.parentElement?.parentElement?.innerText.slice(0,800))));console.log('VISIBLE DATES',(await page.locator('table').last().innerText()).slice(0,1400));console.log('TABLE TEXT',(await page.locator('table').allTextContents()).map(t=>t.slice(0,300)));console.log('MONTH HEADERS',(await page.locator('body').innerText()).match(/(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro) de 20\\d{2}/gi)?.slice(0,15));console.log('ARIA BUTTONS',(await page.locator('button[aria-label]').evaluateAll(bs=>bs.map(b=>b.getAttribute('aria-label')))).slice(-50));if(await page.locator('table[role="grid"] [data-date]').filter({visible:true}).count())break}}catch{}}
  if(!/BRL/.test(await page.locator('body').innerText()))throw new Error('Moeda BRL não confirmada; coleta cancelada.');
  const calendarCount=()=>page.locator('table[role="grid"] [data-date]').count();
  if(!(await calendarCount())){
    for(const selector of ['button:has-text("Visualizar preços")','button:has-text("Selecionar datas")','[data-testid="searchbox-dates-container"]']){
      try{const button=page.locator(selector).first();if(await button.count()){await button.click({timeout:3000});await page.waitForTimeout(1400);if(await calendarCount())break}}catch(e){console.log('Calendário:',selector,String(e).slice(0,100))}
    }
  }
  console.log('CALENDAR DIAGNOSTIC',hotel.name,'cells',await calendarCount(),'grid',await page.locator('table[role="grid"]').count(),'text',(await page.locator('table[role="grid"]').allTextContents()).slice(0,2).map(x=>x.slice(0,220)));

  for(let i=0;i<13;i++){
   await page.waitForFunction(()=>[...document.querySelectorAll('table[role=\"grid\"] [data-date][aria-describedby]')].some(el=>(el.getAttribute('aria-describedby')||'').split(/\s+/).some(id=>/[0-9—]/.test(document.getElementById(id)?.innerText||''))),null,{timeout:8000}).catch(()=>{});
   const found=await page.evaluate(collectScript);
   Object.assign(all,found);
   console.log(hotel.name,'passo',i,'datas',Object.keys(all).length);
   const next=page.getByRole('button',{name:/mês seguinte|próximo mês|next month/i}).first();
   if(!await next.count()||!await next.isEnabled()){console.log('NAVEGAÇÃO INDISPONÍVEL',hotel.name);break;}
   await next.click({timeout:5000});
   await page.waitForTimeout(1000);
  }
  if(!Object.values(all).some(x=>x.price!==null)){console.error('Sem tarifas verificadas:',hotel.name,'URL',page.url(),'GRID',await page.locator('table[role="grid"]').count(),'BOTÕES',(await page.locator('button').allTextContents()).filter(x=>/preços|datas/i.test(x)).slice(0,8));process.exitCode=1;continue;}
  const path='data/'+slug+'.json';
  await fs.writeFile(path,JSON.stringify({name:hotel.name,url:hotel.url,updatedAt:new Date().toISOString(),currency:'BRL',adults:2,rooms:1,nights:1,approximate:true,rates:all},null,2));
  console.log('SALVO',path,Object.keys(all).length);
 }catch(e){console.error('Falha',hotel.name,String(e).slice(0,500));process.exitCode=1}
 finally{await page.close()}
}
await browser.close();
