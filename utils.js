const Promise  = require('bluebird'),
      format   = require('date-fns/format'),
      ora      = require('ora'),
      prettyMs = require('pretty-ms'),
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

module.exports.loadDevices = loadDevices;

module.exports.db = db;

module.exports.now = () => format(Date.now(), 'YYYY-MM-DDTHH:mm:ss');

module.exports.lastHour = () => {
    const date = new Date();
    date.setHours(date.getHours() - 1);

    return format(date, 'YYYY-MM-DD HH');
};

module.exports.start = () => Date.now();

module.exports.elapsedTime = function (start, note) {
    const elapsed = Date.now() - start;
    console.log(`${prettyMs(elapsed)} - ${note}`);
};