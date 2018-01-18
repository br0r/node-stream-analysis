const Stream = require('./index');
const os = require('os');

let latestTimeG = {};
new Stream(process.stdin)
  .split()
  .fromJSON()
  .groupBy(['symbol', 'market'], true)
  .reduce(function(x) {
    for (let symbol in x) {
      let latestTime;
      let prices = {};
      let c = 0;
      for (let market in x[symbol]) {
        let v = x[symbol][market];
        if (!latestTime || latestTime - v.time < 1000) {
          prices[market] = v.price;
          latestTime = v.time;
          c++;
        }
      }
      if (latestTime < latestTimeG) {
        continue;
      }
      latestTimeG = latestTime;
      if (c > 1) {
        let pushed = {};
        const market = Object.keys(prices)[0];
        const price = prices[market];
        for (let market2 in prices) {
          if (market === market2) continue;
          const price2 = prices[market2];
          const arbitrage = Math.abs(price - price2) / Math.min(price, price2) * 100;

          if (arbitrage > 1) {
            const str = `${new Date(latestTime)} - ${symbol} - ${market} ${market2} ${arbitrage}`;
            this.push(str);

          }
        }
      }
    }
  })
  .map(x => x + os.EOL)
  .consume(process.stdout);
