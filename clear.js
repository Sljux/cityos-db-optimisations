const {
          db,
          start,
          elapsedTime
      } = require('./utils');

const time = start();

db.getConnectionAsync()
  .then(() => clearDay())
  .then(() => clearWeek())
  .then(() => clearMonth())
  .then(() => clearSensor())
  .finally(() => {
      db.end();
      elapsedTime(time, 'Clearing data')
  });

function clearDay() {
    return db.queryAsync(`DELETE FROM sensor_day WHERE time < DATE_SUB(NOW(), INTERVAL 1 DAY)`)
}

function clearWeek() {
    return db.queryAsync(`DELETE FROM sensor_week WHERE time < DATE_SUB(NOW(), INTERVAL 7 DAY)`)
}

function clearMonth() {
    return db.queryAsync(`DELETE FROM sensor_month WHERE time < DATE_SUB(NOW(), INTERVAL 1 MONTH)`)
}

function clearSensor() {
    return db.queryAsync(`DELETE FROM sensor WHERE add_on < DATE_SUB(NOW(), INTERVAL 1 MONTH)`)
}