const {
          db,
          start,
          elapsedTime,
          generateAvgs
      }       = require('./utils');

let time = start();

db.getConnectionAsync()
  .then(() => generateAvgs(6, 'HOUR'))
  .finally(() => {
      elapsedTime(time, 'Last 6 hours avg', 'sensor_month');
      db.end()
  });
