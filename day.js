const Promise = require('bluebird'),
      u       = require('untab'),
      mysql   = require('mysql'),
      {
          db,
          now,
          start,
          elapsedTime,
          generateAvgs
      }       = require('./utils');

let time = start();

db.getConnectionAsync()
  .then(() => generateAvgs(30, 'MINUTE'))
  .finally(() => {
      elapsedTime(time, 'Last 30min avg');
      db.end()
  });
