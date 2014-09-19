'use strict';

var pg = require('pg');

//conString format "postgres://username:password@localhost/database";
var conString = 'postgres://'
  + process.env.MDB_USERNAME + ':' 
  + process.env.MDB_PASSWORD + '@' 
  + process.env.MDB_ADDRESS + ':' 
  + process.env.MDB_PORT + '/' 
  + process.env.MDB_DATABASE;

if (conString.indexOf('undefined') > -1 ) {
  console.warn('Variables for Postgres missing.');
  process.exit(1);
} else {
  console.log('Postgres: Connecting to ' + process.env.MDB_ADDRESS +' as ' + process.env.MDB_USERNAME);
}

var client = new pg.Client(conString);
client.connect( function (err) {
  if (err) throw err;
});

var query = module.exports.query = function (queryString, callback) {
  client.query(queryString, callback);
}

module.exports.select_all_from = function (tableName, callback) {
  var sql = 'SELECT * FROM ' + tableName;
  query(sql, callback);
}
