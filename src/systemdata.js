/*jshint node: true */

'use strict';

var mdb = require('./mdb_client'),
    userdb = require('./userdb_client'),
    redis_helper = require('./redis_helper'),
    fs = require('fs'),
    eventEmitter = require('events').EventEmitter,
    fs = require('fs'),
    workerEmitter = new eventEmitter(),
    redis = require("redis"),
    client = redis.createClient();



module.exports.mapLocationsIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_location_id, id FROM location', 'locations', callback);
};

module.exports.mapPermissionIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_nyhedsbrev_id, id FROM permission', 'permissions', callback);
};

module.exports.mapInterestIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_interesse_id, id FROM interest', 'interests', callback);
};

module.exports.mapActionTypeIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT description, id FROM action_type', 'action_types', callback);
};

module.exports.mapReasonTypeIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT text, id FROM reason_type', 'reason_types', callback);
};



// mysql> show columns from reason_type;
// +-------+------------------+------+-----+---------+----------------+
// | Field | Type             | Null | Key | Default | Extra          |
// +-------+------------------+------+-----+---------+----------------+
// | id    | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | text  | varchar(255)     | YES  |     |         |                |
// +-------+------------------+------+-----+---------+----------------+

module.exports.convertReasonTypes = function (callback) {

  // We're inserting a reason_type to be used when eg. tbl_user_afmelding has the 'Andet:'-prefix
  insertReasonType('Andet');

  // To convert reason types, we're fetching the user_feedbacks that are submittet without the custom 'Andet:'-prefix
  // (Reasons with 'Andet:' are converted into unsub_reason.custom_reason_text and a action_history)
  mdb.query('SELECT user_feedback FROM tbl_user_afmelding WHERE user_feedback NOT LIKE \'Andet:%\' GROUP BY user_feedback', function (err, result) {

    if (result.rowCount === 0)
      if (callback !== undefined && typeof callback === 'function')
        return callback();

    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_user_afmelding) {
      insertReasonType(tbl_user_afmelding.user_feedback, function () {
        ++done;
        if (done === count)
          if (callback !== undefined && typeof callback === 'function')
            callback();
      });
    });
  });
};

function insertReasonType (text, callback) {
  client.HEXISTS('reason_types', text, function (err, exists) {
    if (exists === 0) {
      userdb.insert('reason_type', { text: text }, function (err, result) {
        client.HSET('reason_types', text, result.insertId, callback);
      });
    } else {
      if (callback !== undefined && typeof callback === 'function')
        callback();
    }
  });
}





// var tbl_interesse_display_type_3 = 'BEM (Kunden i Centrum)',
//     tbl_interesse_display_type_4 = 'Godttip.dk (3P)',
//     tbl_interesse_display_type_5 = 'Ekstra bruger information';

// mysql> show columns from interest;
// +------------------+---------------------+------+-----+---------+----------------+
// | Field            | Type                | Null | Key | Default | Extra          |
// +------------------+---------------------+------+-----+---------+----------------+
// | id               | int(11) unsigned    | NO   | PRI | NULL    | auto_increment |
// | parent_id        | int(11) unsigned    | YES  |     | NULL    |                |
// | name             | varchar(128)        | NO   |     | NULL    |                |
// | display_name     | varchar(255)        | YES  |     |         |                |
// | description      | varchar(255)        | YES  |     |         |                |
// | active           | tinyint(3) unsigned | NO   |     | 1       |                |
// | mdb_interesse_id | int(11)             | YES  |     | NULL    |                |
// +------------------+---------------------+------+-----+---------+----------------+

module.exports.convertInterests = function (callback) {
  mdb.select_all_from('tbl_interesser', function (err, result) {

    if (result.rowCount === 0)
      if (callback !== undefined && typeof callback === 'function')
        return callback();

    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_interesser) {

      client.HEXISTS('interests', tbl_interesser.interesse_id, function (err, exists) {
        if (exists === 0) {
          var interest = {
            name: tbl_interesser.interesse_navn,
            display_name: tbl_interesser.interesse_navn,
            description: tbl_interesser.beskrivelse,
            active: 1,
            mdb_interesse_id: tbl_interesser.interesse_id
          };
          
          userdb.insert('interest', interest, function (err, result) {
            client.HSET('interests', tbl_interesser.interesse_id, result.insertId);
          });
        }
      });

      ++done;
      if (done === count) {
        if (callback !== undefined && typeof callback === 'function')
          callback();
      }
    });
  });
};




module.exports.convertInterestParents = function (callback) {
  mdb.select_all_from('tbl_interesse_parent', function (err, result) {
    
    if (result.rowCount === 0)
      if (callback !== undefined && typeof callback === 'function')
        return callback();

    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_interesse_parent) {
      client.HGET('interests', tbl_interesse_parent.interesse_parent_id, function (err, parent_interest_id) {
        if (err) throw err;
        //if (interest === null) throw new Error('Parent interest ' + tbl_interesse_parent.interesse_parent_id + ' could not be found in UserDB.');
        if (parent_interest_id === null)
          console.log('Parent interest ' + tbl_interesse_parent.interesse_parent_id + ' could not be found in UserDB.');
        else
          userdb.query('UPDATE interest SET parent_id = ' + parent_interest_id + ' WHERE mdb_interesse_id =' + tbl_interesse_parent.interesse_id);
      });

      ++done;
      if (done === count)
        if (callback !== undefined && typeof callback === 'function')
          callback();
    });
  });
};



// mysql> show columns from action_type;
// +-------------+---------------------+------+-----+---------+----------------+
// | Field       | Type                | Null | Key | Default | Extra          |
// +-------------+---------------------+------+-----+---------+----------------+
// | id          | tinyint(3) unsigned | NO   | PRI | NULL    | auto_increment |
// | description | varchar(255)        | NO   |     | NULL    |                |
// +-------------+---------------------+------+-----+---------+----------------+

module.exports.convertActionTypes = function (callback) {
  mdb.select_all_from('tbl_user_action_type', function (err, result) {
    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_user_action_type) {
      client.HEXISTS('action_types', tbl_user_action_type.user_action_type_name, function (err, exists) {
        if (exists === 0) {
          var action_type = {
            description: tbl_user_action_type.user_action_type_name
          };

          userdb.insert('action_type', action_type, function (err, result) {
            client.HSET('action_types', tbl_user_action_type.user_action_type_name, result.insertId);
          });
        }
      });

      ++done;
      if (done === count)
        if (callback !== undefined && typeof callback === 'function')
          callback();
    });
  });
};



// mysql> show columns from location;
// +-----------------+------------------+------+-----+---------+----------------+
// | Field           | Type             | Null | Key | Default | Extra          |
// +-----------------+------------------+------+-----+---------+----------------+
// | id              | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | description     | varchar(255)     | NO   |     |         |                |
// | active          | tinyint(4)       | NO   |     | 1       |                |
// | mdb_location_id | int(11)          | YES  |     | NULL    |                |
// +-----------------+------------------+------+-----+---------+----------------+

module.exports.convertLocations = function (callback) {
  mdb.select_all_from('tbl_location ORDER BY location_id', function (err, result) {
    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_location) {

      client.HEXISTS('locations', tbl_location.location_id, function (err, exists) {
        if (exists === 0) {
          var location = {
            description: tbl_location.location_tekst,
            active: 1,
            mdb_location_id: tbl_location.location_id
          };

          userdb.insert('location', location, function (err, result) {
            client.HSET('locations', tbl_location.location_id, result.insertId);
          });
        }
      });

      ++done;
      if (done === count)
        if (callback !== undefined && typeof callback === 'function')
          callback();
    });
  });
};



// mysql> show columns from permission;
// +-------------------+---------------------+------+-----+---------+----------------+
// | Field             | Type                | Null | Key | Default | Extra          |
// +-------------------+---------------------+------+-----+---------+----------------+
// | id                | int(11) unsigned    | NO   | PRI | NULL    | auto_increment |
// | name              | varchar(255)        | YES  |     | NULL    |                |
// | active            | tinyint(3) unsigned | NO   |     | 1       |                |
// | display_text      | varchar(255)        | YES  |     | NULL    |                |
// | description       | text                | YES  |     | NULL    |                |
// | mdb_nyhedsbrev_id | int(11)             | YES  |     | NULL    |                |
// +-------------------+---------------------+------+-----+---------+----------------+

module.exports.convertPermissions = function (callback) {
  mdb.select_all_from('tbl_nyhedsbrev WHERE permission = 1', function (err, result) {
    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_nyhedsbrev) {

      client.HEXISTS('permissions', tbl_nyhedsbrev.nyhedsbrev_id, function (err, exists) {
        if (exists === 0) {
          var permission = {
            name: tbl_nyhedsbrev.nyhedsbrev_navn,
            active: tbl_nyhedsbrev.enabled,
            display_text: tbl_nyhedsbrev.nyhedsbrev_navn,
            description: tbl_nyhedsbrev.indhold,
            mdb_nyhedsbrev_id: tbl_nyhedsbrev.nyhedsbrev_id
          };

          userdb.insert('permission', permission, function (err, result) {
            client.HSET('permissions', tbl_nyhedsbrev.nyhedsbrev_id, result.insertId);
          });
        }
      });

      ++done;
      if (done === count)
        if (callback !== undefined && typeof callback === 'function')
          callback();
    });
  });
};

