const Promise     = require('bluebird'),
      ProgressBar = require('progress'),
      {
          loadDevices,
          db,
          now,
          start,
          elapsedTime
      }           = require('./utils');

const devices = [];

let time = start();

let bar;

db.getConnectionAsync()
  .then(() => loadDevices())
  .tap(devices => {
      bar = new ProgressBar(':bar :percent elapsed :elapsed eta :eta', { total : devices.length })
  })
  .map(device =>
      loadAvg(device)
          .then(props =>
              saveAvg(device, props).tap(() => bar.tick())
          )
  )
  .finally(() => {
      elapsedTime(time, 'Last 1h avg');
      db.end()
  });

function loadAvg(device) {
    const keys  = Object.keys(device.schema),
          props = {};

    keys.forEach(key => props[key] = sensorAvg(device.id, key));

    return Promise.props(props)
}

function saveAvg(device, props) {
    const values = [];

    Object.keys(props).forEach(sensorId => {
        values.push([device.id, device.group_id, sensorId, props[sensorId], device.user_id, now()])
    });

    return db.queryAsync(`INSERT INTO sensor_month(device_id, group_id, sensor_id, value, user_id, add_on) VALUES ?`, [values])
}

function sensorAvg(deviceId, sensorId) {
    return db.queryAsync(`SELECT AVG(value) AS avg FROM sensor WHERE device_id = ${deviceId} AND sensor_id = ${sensorId} AND add_on <= DATE_SUB(NOW(), INTERVAL 6 HOUR)`)
             .then(result => Math.round(result[0].avg))
}