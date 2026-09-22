// Exercise the actual frontend matcher without browser state or live data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(`${__dirname}/trading-agents-stock.html`, 'utf8');
const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(match => match[1]);
scripts.forEach(script => new Function(script));
const script = scripts.find(script => script.includes('function matchesPredictionSearch('));
const matcher = script.slice(script.indexOf('function matchesPredictionSearch('), script.indexOf('function renderMinutePredictions('));
const context = vm.createContext({});
vm.runInContext(matcher, context);
const stock = {code:'688361', name:'中科飞测', nameInitials:'zkfc'};
for (const keyword of ['zkfc', 'ZKFC', 'kfc', ' FC ', '中科', '飞测', '688361', '8361', '']) {
  assert.equal(context.matchesPredictionSearch(stock, keyword, new Map()), true, keyword);
}
assert.equal(context.matchesPredictionSearch(stock, 'hdzz', new Map()), false);
const cache = new Map([[stock.code, stock]]);
assert.equal(context.matchesPredictionSearch({code:'688361',name:'中科飞测'}, 'zkfc', cache), true);
assert.equal(context.matchesPredictionSearch({code:'688114',name:'华大智造'}, 'zkfc', cache), false);
assert.equal(context.matchesPredictionSearch({}, 'undefined', new Map()), false);
console.log('Prediction search: full/partial initials, case, whitespace, name, code and cached metadata passed.');
