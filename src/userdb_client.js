'use strict';

var mysql = require('mysql');

console.log('MySQL: Connecting to ' + process.env.RDS_HOSTNAME +' as ' + process.env.RDS_USERNAME);

var pool = mysql.createPool({
  host: process.env.RDS_HOSTNAME,
  port: process.env.RDS_PORT ? process.env.RDS_PORT : null,
  user: process.env.RDS_USERNAME,
  password: process.env.RDS_PASSWORD,
  database: process.env.RDS_DATABASE ? process.env.RDS_DATABASE : null 
});

// Testing we can connect to database
pool.getConnection(function(err, connection) {
  if (err) {
    console.log('Connection to MySQL failed: ', err)
    process.exit(1);
  } else {
    connection.release();
  }
});


function insertSqlString (tableName, data) {
  var columns = [], values = [];
  for (var column in data) {
    columns.push(column);
    values.push(pool.escape(data[column]));
  }

  return 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES (' + values.join(',') + ')';
}


function updateSqlString (tableName, data) {
  var pairs = [];
  for (var column in data) {
    if (column !== 'id')
      pairs.push(column + '=' + pool.escape(data[column]));
  }

  return 'UPDATE ' + tableName + ' SET ' + pairs.join(',') + ' WHERE id = ' + data['id'];
}


module.exports.query = function (query, callback) {
  pool.query(query, callback);
}


module.exports.insert = function (tableName, data, callback) {
  // if (callback !== undefined && typeof callback === 'function') {
  //   //console.log('insert', tableName, data);
  //   console.log('insert', tableName);
  //   callback(null, {insertId: 1});
  // }

  var sql = insertSqlString(tableName, data);
  pool.query(sql, function (err, result) {
    if (err)
    {
      console.log('Error when inserting into ' + tableName, data);
      throw err;
    }
    else if (callback !== undefined && typeof callback === 'function')
     callback(err, result);
  });
}


module.exports.update = function (tableName, data, callback) {

  if (data['id'] === undefined) {
    callback('Field id missing.');
  }

  var sql = updateSqlString(tableName, data);
  console.log('test update', sql);
  //pool.query(sql, callback);
}


module.exports.select_all_from = function (tableName, callback) {
  var sql = 'SELECT * FROM ' + tableName;
  pool.query(sql, callback);
}


module.exports.select_id_from = function (tableName, callback) {
  var sql = 'SELECT id FROM ' + tableName;
  pool.query(sql, callback);
}

module.exports.select_member_email = function (member_id, callback) {
  var sql = 'SELECT member.id, email.email_address FROM member LEFT JOIN email on member.id = email.member_id WHERE member.id=' + member_id;
  pool.query(sql, callback);
}