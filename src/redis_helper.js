'use strict';

var mdb = require('./mdb_client'),
    userdb = require('./userdb_client'),
    eventEmitter = require('events').EventEmitter,
    redis = require("redis"),
    client = redis.createClient();


module.exports.createListCopyFromMdb = function (tableName, listName, callback) {
  var limit = 1000,
      offset = 0,
      ee = new eventEmitter();

  ee.on('next', readandimport);
  ee.on('done', callback);

  client.DEL(listName, function (err, result) {
    readandimport();
  });

  function readandimport () {
    mdb.select_all_from( tableName + ' LIMIT ' + limit + ' OFFSET ' + offset, function (err, result) {
      if (err) throw err;
      
      var count = result.rowCount,
          done = 0;

      if (result.rowCount === 0) {
        ee.emit('done');
      }

      result.rows.forEach(function (row) {
        client.LPUSH(listName, JSON.stringify(row));
        ++done;

        if (done === count) {
          console.log('Done ' + listName, count, offset, limit);
          ee.emit('next');
        }
      });
      offset = offset + limit;
    });
  }  
}


module.exports.createHashMappingFromUserdb = function (sql, hashName, callback) {
  userdb.query(sql, function (err, data) {
    if (err) throw err;

    var count = data.length,
        done = 0;

    console.log('Importing ' + count + ' into hash ' + hashName + '.');

    client.DEL(hashName, function (err, result) {

      if (count === 0)
        callback();

      data.forEach(function (item) {
        client.HSET(hashName, item[Object.keys(item)[0]], item[Object.keys(item)[1]], function (err, result) {
          ++done;
          if (done === count) {
            console.log('Hash ' + hashName + ' done.');
            if (callback !== undefined && typeof callback === 'function')
              callback();
          }

          if (done % 1000 === 0)
            console.log('Hash ' + hashName + ' imported: ' + done + '/' + count + '.');
        });
      });
    });

  });
}

module.exports.test = function () {
  client.DEL('test', 'test_done', console.log);
}