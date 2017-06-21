const {
          db,
          start,
          elapsedTime,
          generateAvgs
      }       = require('./utils');

let time = start();

db.getConnectionAsync()
  .then(() => generateAvgs(6, 'HOUR', 'sensor_month'))
  .finally(() => {
      elapsedTime(time, 'Last 6 hours avg');
      db.end()
  });
