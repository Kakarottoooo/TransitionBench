import {chromium} from '../web/node_modules/playwright-core/index.mjs';
import path from 'node:path';
import fs from 'node:fs/promises';

const root=path.resolve(import.meta.dirname,'..');
const browser=await chromium.launch({channel:'chrome',headless:true});
const context=await browser.newContext({viewport:{width:1440,height:1000},recordVideo:{dir:path.join(root,'work','recording'),size:{width:1440,height:1000}}});
const page=await context.newPage();
try{
  await page.goto('http://127.0.0.1:8765');
  await page.waitForTimeout(1800);
  await page.getByRole('combobox',{name:'Workload',exact:true}).selectOption('mixed-burst');
  const rate=page.getByRole('slider',{name:'Arrival rate',exact:true});
  await rate.focus();await rate.press('Home');for(let i=0;i<7;i++)await rate.press('ArrowRight');
  await page.getByRole('button',{name:'Compare four policies'}).click();
  await page.getByRole('status',{name:'Experiment status'}).filter({hasText:'Four runs computed'}).waitFor({timeout:30000});
  await page.getByRole('img',{name:'Cumulative SLO-qualified completions from raw events'}).scrollIntoViewIfNeeded();
  await page.waitForTimeout(2200);
  await page.getByRole('button',{name:'Steady-state first',exact:true}).click();
  await page.waitForTimeout(500);
  await page.screenshot({path:path.join(root,'demo','transition-detail.png'),fullPage:true});
  await page.getByRole('heading',{name:'One worker changes. One keeps serving.'}).scrollIntoViewIfNeeded();
  await page.waitForTimeout(2200);
  await page.getByRole('button',{name:'03 Inspect Evidence'}).click();
  await page.getByRole('heading',{name:'The number is only the beginning.'}).scrollIntoViewIfNeeded();
  await page.waitForTimeout(2200);
  await page.getByRole('heading',{name:'Raw event excerpt'}).scrollIntoViewIfNeeded();
  await page.waitForTimeout(1800);
  await page.getByRole('button',{name:'02 Run on Your Setup'}).click();
  await page.getByRole('heading',{name:'Bring your evidence. Keep your deployment authority.'}).scrollIntoViewIfNeeded();
  await page.waitForTimeout(2200);
  await page.screenshot({path:path.join(root,'demo','setup-desktop.png'),fullPage:true});
}finally{
  const video=page.video();await context.close();
  await fs.copyFile(await video.path(),path.join(root,'demo','walkthrough.webm'));
  await browser.close();
}
console.log('Recorded actual local UI interaction to demo/walkthrough.webm');
