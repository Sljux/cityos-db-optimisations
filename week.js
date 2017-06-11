const vogels      = require('vogels'),
      Joi         = require('joi'),
      Promise     = require('bluebird'),
      ProgressBar = require('progress'),
      {
          loadDevices,
          db,
          now,
          lastHour,
          start,
          elapsedTime
      }           = require('./utils');

vogels.AWS.config.update({ region : 'us-east-1' });

const Sensor = vogels.define('sensor', {
    hashKey    : 'id',
    timestamps : true,
    updatedAt  : false,
    tableName  : 'sensor',

    schema : {
        id       : vogels.types.uuid(),
        time     : Joi.string(),
        deviceId : Joi.number().integer().min(1),
        sensorId : Joi.number().integer().min(1),
        groupId  : Joi.number().integer().min(1),
        value    : Joi.number(),
        userId   : Joi.number()
    }
});

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
    const values = [],
          models = [];

    Object.keys(props).forEach(sensorId => {
        values.push([device.id, device.group_id, sensorId, props[sensorId], device.user_id, now()]);
        models.push({
            value    : Math.floor(props[sensorId]) / 1000,
            time     : lastHour(),
            deviceId : device.id,
            sensorId : sensorId,
            groupId  : device.group_id,
            userId   : device.user_id
        })
    });

    console.log(props, values, models);

    return db.queryAsync(`INSERT INTO sensor_week(device_id, group_id, sensor_id, value, user_id, add_on) VALUES ?`, [values])
             .then(() =>
                 new Promise((resolve, reject) => {
                     Sensor.create(models, (err) => {
                         if (err) reject(err);

                         resolve();
                     })
                 })
             )
}

function sensorAvg(deviceId, sensorId) {
    return db.queryAsync(`SELECT AVG(value) AS avg FROM sensor WHERE device_id = ${deviceId} AND sensor_id = ${sensorId} AND add_on <= DATE_SUB(NOW(), INTERVAL 1 HOUR)`)
             .then(result => Math.round(result[0].avg))
}
