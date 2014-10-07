/*jshint node: true */

'use strict';

var mdb = require('./mdb_client'),
    userdb = require('./userdb_client'),
    eventEmitter = require('events').EventEmitter,
    redis = require("redis"),
    client = redis.createClient();


module.exports.cacheLocations = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_location_id, id FROM location', 'locations', callback);
};

module.exports.cachePermission = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_nyhedsbrev_id, id FROM permission', 'permissions', callback);
};

module.exports.cacheInterest = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_interesse_id, id FROM interest', 'interests', callback);
};

module.exports.cacheActionType = function (callback) {
  createHashMappingFromUserdb('SELECT description, id FROM action_type', 'action_types', callback);
};

module.exports.cacheReasonType = function (callback) {
  createHashMappingFromUserdb('SELECT text, id FROM reason_type', 'reason_types', callback);
};

module.exports.cachePublisher = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_publisher_id, id FROM publisher', 'publishers', callback);
};

module.exports.cacheSubscription = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_nyhedsbrev_id, id FROM subscription', 'subscriptions', callback);
};

module.exports.copySignups = function (callback) {
  createListCopyFromMdb('tbl_signup_nyhedsbrev', 'tbl_signup_nyhedsbrev', callback);
};

module.exports.copySignouts = function (callback) {
  createListCopyFromMdb('tbl_user_afmelding', 'tbl_user_afmelding', callback);
};

module.exports.copyOptOuts = function (callback) {
  createListCopyFromMdb('tbl_mail_optout', 'tbl_mail_optout', callback);
};

module.exports.cacheEmails = function (callback) {
  createHashMappingFromUserdb('SELECT member_id, id FROM email', 'emails', callback);
};

module.exports.cacheEmailIds = function (callback) {
  createHashMappingFromUserdb('SELECT email_address, id FROM email', 'email_ids', callback);
};

module.exports.cacheSubscriptionMembers = function (callback) {
  createSetMappingFromUserDB('SELECT member_id, subscription_id FROM subscription_member', 'subscription_member', callback);
};

module.exports.cachePermissionMembers = function (callback) {
  createSetMappingFromUserDB('SELECT member_id, permission_id FROM permission_member', 'permission_member', callback);
};

module.exports.cacheOptOuts = function (callback) {
  createSetFromUserDB('SELECT email_id FROM opt_outs', 'opt_outs', callback);
};

module.exports.copyBrugere = function (callback) {
  createListCopyFromMdb('tbl_bruger', 'tbl_bruger', callback);
};

module.exports.copyInteresseLinier = function (callback) {
  createListCopyFromMdb('tbl_interesse_linie', 'tbl_interesse_linie', callback);
};

module.exports.copyUserActions = function (callback) {
  createListCopyFromMdb('tbl_user_action', 'tbl_user_action', callback);
};

module.exports.cacheMembers = function (callback) {
  createHashMappingFromUserdb('SELECT mdb_user_id, id FROM member', 'members', callback);
};

module.exports.cacheInterestLines = function (callback) {
  createSetMappingFromUserDB('SELECT member_id, interest_id FROM interest_line', 'interest_line', callback);
};


// Jeg opretter et SET med mdb_user_id fra UserDB.
// Og et SET med user_id fra MDB.
// MED SDIFFSTORE beregner redis forskelle og gemmer de manglende bruger id'er i et nyt SET

module.exports.diffMembers = function (callback) {
  userdb.query('SELECT mdb_user_id FROM member', function (err, members) {
    if (err) throw err;

    redis_helper.createSet(member, 'smembers', function () {

      mdb.query('SELECT user_id FROM tbl_bruger', function (err, brugere) {
        if (err) throw err;

        redis_helper.createSet(brugere.rows, 'sbrugere', function () {

          console.log('Diffing!');
          client.SDIFFSTORE('smissing', 'sbrugere', 'smembers', function (err, result) {
            if (err) throw err;

            console.log('Result from SDIFFSTORE:', result);

            callback();
          });
        });
      });
    });
  })
}


function createListCopyFromMdb (tableName, listName, callback) {
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
      
      if (result.rowCount === 0) {
        ee.emit('done');
      }

      var count = result.rowCount,
          done = 0;

      result.rows.forEach(function (row) {
        client.LPUSH(listName, JSON.stringify(row), function (err, result) {
          ++done;

          if (done === count) {
            console.log('Imported', count, 'into', listName, '. Continuing from', offset);
            ee.emit('next');
          }
        });
      });
      offset = offset + limit;
    });
  }  
};


function createHashMappingFromUserdb (sql, hashName, callback) {
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
          if (err) throw err;

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
};


function createSetMappingFromUserDB (sql, setPrefix, callback) {
  userdb.query(sql, function (err, data) {
    if (err) throw err;

    var count = data.length,
        done = 0;

    console.log('Importing ' + count + ' into sets with prefix ' + setPrefix + '.');

    data.forEach(function (item) {
      var skey = setPrefix + ':' + item[Object.keys(item)[0]],
          smember = item[Object.keys(item)[1]];

      client.DEL(skey, function (err, result) {
        if (err) throw err;

        client.SADD(skey, smember, function (err, result) {
          if (err) throw err;

          ++done;
          if (done === count) {
            console.log('Sets ' + setPrefix + ' done.');
            if (callback !== undefined && typeof callback === 'function')
              callback();
          }

          if (done % 1000 === 0)
            console.log('Sets ' + setPrefix + ' imported: ' + done + '/' + count + '.');
        });
      });
    });
  });
};


function createSetFromUserDB (sql, setName, callback) {
  userdb.query(sql, function (err, data) {
    if (err) throw err;

    createSet(data, callback);
  });
};


function createSet (data, setName, callback) {
  var count = data.length,
      done = 0;

  console.log('Importing ' + count + ' into set ' + setName + '.');
  
  client.DEL(setName, function (err, result) {
    if (err) throw err;

    data.forEach(function (item) {
      var smember = item[Object.keys(item)[0]];

      client.SADD(setName, smember, function (err, result) {
        if (err) throw err;

        ++done;
        if (done === count) {
          console.log('Set ' + setName + ' done.');
          if (callback !== undefined && typeof callback === 'function')
            callback();
        }

        if (done % 1000 === 0)
          console.log('Set ' + setName + ' imported: ' + done + '/' + count + '.');
      });
    });
  });
};