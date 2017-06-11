const vogels      = require('vogels'),
      ora         = require('ora'),
      Promise     = require('bluebird'),
      ProgressBar = require('progress'),
      chunk       = require('lodash/chunk'),
      Joi         = require('joi'),
      {
          db,
          loadDevices,
          start,
          elapsedTime
      }           = require('./utils');

vogels.AWS.config.loadFromPath('credentials.json');
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

const time = start();

initHistory();

async function initHistory() {
    const conn = await db.getConnectionAsync();

    const devices = devicesMap(await loadDevices());

    const spinner = ora('Performing grouping').start();

    const sql = `
        SELECT AVG(value) AS value, DATE(add_on) AS date, HOUR(add_on) AS hour, device_id, sensor_id
        FROM sensor 
        WHERE add_on >= '2017-06-05 14:40:10'
        GROUP BY DATE(add_on), HOUR(add_on), device_id, sensor_id
        -- LIMIT 100
    `;

    const results = await db.queryAsync(sql);

    spinner.succeed('Done - streaming results to DynamoDB');

    const bar = new ProgressBar(':bar :percent elapsed :elapsed eta :eta', { total : results.length });

    await Promise.each(
        chunk(results, 25),
        rows => {
            const models = rows.map(row => rowToModel(row, devices));

            return new Promise((resolve, reject) => {
                Sensor.create(models, (err) => {
                    if (err) reject(err);

                    bar.tick(models.length);

                    resolve();
                })
            })
        }
    );

    conn.release();
    db.end();

    elapsedTime(time, 'History initialisation');
}

function devicesMap(devices) {
    const result = {};

    devices.forEach(d => result[d.id] = d);

    return result
}

function rowToModel(row, devices) {
    return {
        value    : Math.floor(row.value) / 1000,
        time     : `${row.date} ${row.hour}`,
        deviceId : row.device_id,
        sensorId : row.sensor_id,
        groupId  : devices[row.device_id].group_id,
        userId   : devices[row.device_id].user_id
    }
}