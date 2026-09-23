import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'e2e',timeout:60000,workers:1,reporter:[['list'],['json',{outputFile:'../reports/browser-results.json'}]],use:{baseURL:'http://127.0.0.1:8765',headless:true,channel:'chrome',viewport:{width:1440,height:1050},screenshot:'only-on-failure',trace:'retain-on-failure'},outputDir:'../work/browser-results'});
