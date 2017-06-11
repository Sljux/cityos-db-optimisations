const Promise     = require('bluebird'),
      mysql       = require('mysql'),
      ProgressBar = require('progress'),
      {
          db,
          loadDevices,
          now,
          start,
          elapsedTime
      }           = require('./utils');

let time = start();

db.getConnectionAsync()
  .then(() => initDay())
  .then(() => initWeek())
  .then(() => initMonth())
  .finally(() => {
      db.end();
      elapsedTime(time, 'Initialisation')
  });

function initDay() {
    console.log('Initialising day table');

    let bar;

    return loadDevices()
        .tap(devices => {
            bar = new ProgressBar(':bar :percent elapsed :elapsed eta :eta', { total : devices.length })
        })
        .map(device =>
            Promise.resolve(Object.keys(device.schema))
                   .map(sensorId => initDayDevice(device, sensorId))
                   .tap(() => {
                       bar.tick();
                   })
        )
}

function sql(device, sensorId, days, groupBy, time) {
    return `
        SELECT ${time} AS time, AVG(value) AS avg, ${groupBy} 
        FROM sensor
        WHERE device_id = ${device.id} AND sensor_id = ${sensorId} AND add_on >= DATE_SUB(NOW(), INTERVAL ${days} DAY)
        GROUP BY ${groupBy};
    `
}

function insertValues(result, device, sensorId, table) {
    const sql = `INSERT INTO ${table} (add_on, device_id, group_id, sensor_id, value, user_id, time) VALUES ?`;

    const values = [];

    result.forEach(r => values.push([now(), device.id, device.group_id, sensorId, Math.round(r.avg), device.user_id, r.time]));

    return db.queryAsync(sql, [values])
}

function initDayDevice(device, sensorId) {
    const groupBy = `DATE_FORMAT(add_on, '%Y-%m-%d %H:00:00')`;

    return db.queryAsync(sql(device, sensorId, 2, groupBy, groupBy))
             .then(result => {
                 if (result.length === 0) return;
                 return insertValues(result, device, sensorId, 'sensor_day');
             })
}

function initWeek() {
    console.log('Initialising week table');

    let bar;

    return loadDevices()
        .tap(devices => {
            bar = new ProgressBar(':bar :percent elapsed :elapsed eta :eta', { total : devices.length })
        })
        .map(device =>
            Promise.resolve(Object.keys(device.schema))
                   .map(sensorId => initWeekDevice(device, sensorId))
                   .tap(() => {
                       bar.tick();
                   })
        )
}

function initWeekDevice(device, sensorId) {
    const groupBy = `DATE_FORMAT(add_on, '%Y-%m-%d %H:00:00')`;

    return db.queryAsync(sql(device, sensorId, 7, groupBy, groupBy))
             .then(result => {
                 if (result.length === 0) return;
                 return insertValues(result, device, sensorId, 'sensor_week');
             })
}

function initMonth() {
    console.log('Initialising month table');

    let bar;

    return loadDevices()
        .tap(devices => {
            bar = new ProgressBar(':bar :percent elapsed :elapsed eta :eta', { total : devices.length })
        })
        .map(device =>
            Promise.resolve(Object.keys(device.schema))
                   .map(sensorId => initMonthDevice(device, sensorId))
                   .tap(() => {
                       bar.tick();
                   })
        )
}

function initMonthDevice(device, sensorId) {
    const groupBy = `FLOOR(((HOUR(add_on) + 6) % 24) / 12)`,
          time    = `CONCAT(DATE(add_on), ' ', IF(floor(((hour(add_on) + 6) % 24) / 12) = 0, '06:00:00', '18:00:00'))`;

    return db.queryAsync(sql(device, sensorId, 30, groupBy, time))
             .then(result => {
                 if (result.length === 0) return;
                 return insertValues(result, device, sensorId, 'sensor_month')
             })
}