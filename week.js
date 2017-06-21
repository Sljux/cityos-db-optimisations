const vogels  = require('vogels'),
      Joi     = require('joi'),
      Promise = require('bluebird'),
      {
          db,
          lastHour,
          start,
          elapsedTime,
          now,
          query,
          insert,
          rowToInsert
      }       = require('./utils');

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

let time = start();

db.getConnectionAsync()
  .then(() => query(1, 'HOUR'))
  .tap(saveToDb)
  .tap(saveHistory)
  .finally(() => {
      elapsedTime(time, 'Last 1h avg');
      db.end()
  });

function saveToDb(values) {
    return Promise.map(values, rowToInsert).then(values => insert(values, 'sensor_week'))
}

function saveHistory(values) {
    return Promise.map(values, rowToDynamo).then(saveToDynamo)
}

function rowToDynamo(row) {
    return {
        value    : Math.floor(row.avg) / 1000,
        time     : lastHour(),
        deviceId : row.device_id,
        sensorId : row.sensor_id,
        groupId  : row.group_id,
        userId   : row.user_id
    }
}

function saveToDynamo(values) {
    return new Promise((resolve, reject) => {
        Sensor.create(values, err => {
            if (err) return reject(err);

            resolve()
        })
    })
}
