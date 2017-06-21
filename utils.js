const Promise  = require('bluebird'),
      format   = require('date-fns/format'),
      ora      = require('ora'),
      prettyMs = require('pretty-ms'),
      u        = require('untab'),
      mysql    = require('mysql');

Promise.promisifyAll(require('mysql/lib/Connection').prototype);
Promise.promisifyAll(require('mysql/lib/Pool').prototype);

const dbOptions = {
    host            : 'localhost',
    user            : 'root',
    password        : '',
    database        : 'grid',
    connectionLimit : 10,
    dateStrings     : true
};

const db = mysql.createPool(dbOptions);

let devices = null;

const now = () => format(Date.now(), 'YYYY-MM-DDTHH:mm:ss');

function loadDevices() {
    const spinner = ora('Fetching devices').start();

    if (devices) {
        spinner.succeed('Returning cached devices');

        return Promise.resolve(devices);
    }

    return db.queryAsync(`SELECT id, user_id, group_id, schema_id FROM device`)
             .tap(() => {
                 spinner.text = 'Fetching schemas'
             })
             .map(device => {
                 return db.queryAsync(`SELECT layout FROM \`schema\` WHERE id = ${device.schema_id}`)
                          .then(schema => Object.assign({}, device, { schema : JSON.parse(schema[0].layout).sense }))
             })
             .tap(() => spinner.succeed('Fetched devices and their schemas'))
             .tap(res => devices = res)
}

function query(count, interval) {
    const sql = u`
    SELECT ROUND(AVG(value)) AS avg, device_id, sensor_id, group_id, user_id
    FROM sensor
    WHERE add_on >= DATE_SUB(NOW(), INTERVAL ${count} ${interval.toUpperCase()})
    GROUP BY device_id, sensor_id
    `;

    return db.queryAsync(sql)
}

function rowToInsert(row) {
    return [row.device_id, row.group_id, row.sensor_id, row.avg, row.user_id, now(), now()]
}

function insert(values, table) {
    const sql = u`INSERT INTO ${table}(device_id, group_id, sensor_id, value, user_id, add_on, time) VALUES ?`;

    return db.queryAsync(sql, [values])
}

function queryAndInsert(count, interval, table) {
    return query(count, interval)
        .map(rowToInsert)
        .then(values => insert(values, table))
}

module.exports.rowToInsert = rowToInsert;

module.exports.query = query;

module.exports.insert = insert;

module.exports.generateAvgs = queryAndInsert;

module.exports.loadDevices = loadDevices;

module.exports.db = db;

module.exports.now = now;

module.exports.lastHour = () => {
    const date = new Date();
    date.setHours(date.getHours() - 1);

    return format(date, 'YYYY-MM-DD HH');
};

module.exports.start = () => Date.now();

module.exports.elapsedTime = function (start, note) {
    const elapsed = Date.now() - start;
    console.log(`${now()} ${prettyMs(elapsed)} - ${note}`);
};