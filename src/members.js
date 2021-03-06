/*jshint node: true */

'use strict';

var mdb = require('./mdb_client'),
    userdb = require('./userdb_client'),
    fs = require('fs'),
    eventEmitter = require('events').EventEmitter,
    workerEmitter = new eventEmitter(),
    redis = require("redis"),
    client = redis.createClient();
    // error_queue = 'convert_error',
    // skipped_queue = 'convert_skipped',


var exiting = false;
process.on('SIGINT', function() {
  if (exiting) process.exit(0); // If pressed twice.
  exiting = true;
});

var phone_type_telefon = 1;
var phone_type_mobil = 2;
var system_id_mdb_sync = 1;





workerEmitter.on('get_ready', createOrFindTelefonPhoneType);
workerEmitter.on('telefon_ready', createOrFindMobilPhoneType);
workerEmitter.on('mobil_ready', createMdbSyncSystem);
workerEmitter.on('system_ready', function () {
  workerEmitter.emit('ready');
});


module.exports.convertMembers = function (callback) {
  var convertedMembers = 0,
      start = Date.now(),
      splitTime = start;

  // Starting the whole thing!
  workerEmitter.emit('get_ready');
  workerEmitter.on('ready', work);

  function work () {
    if (exiting) {
      console.log('Clean exit.');
      process.exit(0);
    }

    client.RPOP('tbl_bruger', function (err, bruger) {
      if (err) throw err;

      if (bruger === null)
        return callback();
      else {
        var tbl_bruger = JSON.parse(bruger);
        convertMember(tbl_bruger, function () {

          if (++convertedMembers % 1000 === 0) {
            client.LLEN('tbl_bruger', function (err, data) {
              var temp = Date.now();
              console.log('Items to go from tbl_bruger:', data, 'Members converted:', convertedMembers);
              console.log('Time since start:', (temp - start) / 1000, 'seconds. 1000 in', (temp - splitTime) / 1000, 'seconds.');
              splitTime = temp;
            });
          }

          workerEmitter.emit('ready');
        });
      }
    });
  }
};


module.exports.convertMissingMembers = function (callback) {
  var convertedMembers = 0;

  // Starting the whole thing!
  workerEmitter.emit('get_ready');
  workerEmitter.on('ready', work);

  function work () {
    if (exiting) {
      console.log('Clean exit.');
      process.exit(0);
    }

    client.SPOP('smissing_mysql', function (err, user_id) {
      if (err) throw err;

      if (user_id === null)
        return callback();
      else {
        mdb.query('SELECT * FROM tbl_bruger WHERE user_id = ' + user_id, function (err, result) {

          if (result.rowCount !== 1) {
            throw new Error('user_id too many ' + user_id + ' ' + result.rowCount);
          }

          convertMember(result.rows[0], function () {

            if (++convertedMembers % 1000 === 0) {
              console.log(convertedMembers, 'done /');
            }

            workerEmitter.emit('ready');
          });
        });
      }
    });
  }
};





// mysql> show columns from member;
// +---------------+---------------------------+------+-----+-------------------+----------------+
// | Field         | Type                      | Null | Key | Default           | Extra          |
// +---------------+---------------------------+------+-----+-------------------+----------------+
// | id            | int(11) unsigned          | NO   | PRI | NULL              | auto_increment |
// | firstname     | varchar(255)              | YES  |     |                   |                |
// | lastname      | varchar(255)              | YES  |     |                   |                |
// | coname        | varchar(255)              | YES  |     |                   |                |
// | birth_year    | year(4)                   | YES  |     | NULL              |                |
// | birth_date    | date                      | YES  |     | NULL              |                |
// | gender        | char(1)                   | YES  |     |                   |                |
// | username      | varchar(255)              | YES  |     |                   |                |
// | password      | varchar(255)              | YES  |     |                   |                |
// | status        | enum('inactive','active') | NO   |     | inactive          |                |
// | company       | varchar(255)              | YES  |     |                   |                |
// | company_cvr   | varchar(255)              | YES  |     |                   |                |
// | is_internal   | tinyint(1)                | NO   |     | 0                 |                |
// | robinson_flag | tinyint(1) unsigned       | NO   |     | 0                 |                |
// | created_at    | timestamp                 | YES  |     | CURRENT_TIMESTAMP |                |
// | activated_at  | datetime                  | YES  |     | NULL              |                |
// | updated_at    | datetime                  | YES  |     | NULL              |                |
// | mdb_user_id   | int(11)                   | YES  |     | NULL              |                |
// +---------------+---------------------------+------+-----+-------------------+----------------+

function convertMember (tbl_bruger, callback) {
  client.HEXISTS('members', tbl_bruger.user_id, function (err, result) {
    if (result === 1) {
      callback();
      return;
    }

    var member = {
      mdb_user_id: tbl_bruger.user_id,
      firstname: tbl_bruger.fornavn,
      lastname: tbl_bruger.efternavn,
      coname: tbl_bruger.co_navn,
      birth_year: tbl_bruger.foedselsaar,
      birth_date: tbl_bruger.foedselsdato,
      gender: tbl_bruger.koen,
      username: tbl_bruger.brugernavn,
      password: tbl_bruger.adgangskode,
      status: tbl_bruger.active === true ? 'active' : 'inactive',
      company: tbl_bruger.firma,
      company_cvr: null, // TODO
      is_internal: '0', // TODO: Test på email adresse eller lignede?
      robinson_flag: tbl_bruger.robinson_flag === true ? '1' : '0',
      activated_at: tbl_bruger.activate_dato,  // 'f' eller 't'
      updated_at: tbl_bruger.opdatering_dato
    };

    userdb.insert('member', member, function (err, result) {
      var member_id = result.insertId;

      convertEmail(tbl_bruger, member_id, function () {
        convertTelefon(tbl_bruger, member_id, function () {
          convertMobil(tbl_bruger, member_id, function () {
            convertAddress(tbl_bruger, member_id, function () {
              convertForeignKey(tbl_bruger, member_id, function () {
                callback();
              });
            });
          });
        });
      });
    });
  });
}


// mysql> show columns from email;
// +---------------+---------------------+------+-----+-------------------+----------------+
// | Field         | Type                | Null | Key | Default           | Extra          |
// +---------------+---------------------+------+-----+-------------------+----------------+
// | id            | int(11) unsigned    | NO   | PRI | NULL              | auto_increment |
// | member_id     | int(11) unsigned    | NO   | MUL | NULL              |                |
// | email_address | varchar(255)        | YES  |     |                   |                |
// | system_id     | int(11) unsigned    | NO   | MUL | NULL              |                |
// | active        | tinyint(3) unsigned | NO   |     | 1                 |                |
// | created_at    | timestamp           | YES  |     | CURRENT_TIMESTAMP |                |
// | updated_at    | datetime            | YES  |     | NULL              |                |
// +---------------+---------------------+------+-----+-------------------+----------------+

function convertEmail (tbl_bruger, member_id, callback) {
  var email = {
    member_id: member_id,
    email_address: tbl_bruger.email,
    system_id: system_id_mdb_sync,
    active: 1
  };

  userdb.insert('email', email, callback);
}


// mysql> show columns from phone;
// +------------+------------------+------+-----+-------------------+----------------+
// | Field      | Type             | Null | Key | Default           | Extra          |
// +------------+------------------+------+-----+-------------------+----------------+
// | id         | int(11) unsigned | NO   | PRI | NULL              | auto_increment |
// | member_id  | int(11) unsigned | NO   | MUL | NULL              |                |
// | type_id    | int(11) unsigned | NO   | MUL | NULL              |                |
// | system_id  | int(11) unsigned | NO   | MUL | NULL              |                |
// | number     | varchar(50)      | NO   |     |                   |                |
// | status     | tinyint(4)       | NO   |     | NULL              |                |
// | created_at | timestamp        | YES  |     | CURRENT_TIMESTAMP |                |
// +------------+------------------+------+-----+-------------------+----------------+

function convertTelefon (tbl_bruger, member_id, callback) {
  if (tbl_bruger.telefon !== null && tbl_bruger.telefon !== '') {

    var telefon = {
      member_id: member_id,
      type_id: phone_type_telefon,
      system_id: system_id_mdb_sync,
      number: tbl_bruger.telefon,
      status: 1
    };

    userdb.insert('phone', telefon, callback);
  } else {
    callback();
  }
}

function convertMobil (tbl_bruger, member_id, callback) {
  if (tbl_bruger.mobil !== null && tbl_bruger.mobil !== '') {

    var mobil = {
      member_id: member_id,
      type_id: phone_type_mobil,
      system_id: system_id_mdb_sync,
      number: tbl_bruger.mobil,
      status: 1
    };

    userdb.insert('phone', mobil, callback);
  } else {
    callback();
  }
}


// mysql> show columns from address;
// +---------------+----------------------------+------+-----+---------+----------------+
// | Field         | Type                       | Null | Key | Default | Extra          |
// +---------------+----------------------------+------+-----+---------+----------------+
// | id            | int(11) unsigned           | NO   | PRI | NULL    | auto_increment |
// | member_id     | int(11) unsigned           | NO   | MUL | NULL    |                |
// | active        | tinyint(1) unsigned        | NO   |     | 1       |                |
// | type          | enum('billing','shipping') | NO   |     | billing |                |
// | system_id     | int(11) unsigned           | NO   | MUL | NULL    |                |
// | road_name     | varchar(255)               | YES  |     |         |                |
// | house_number  | varchar(10)                | YES  |     |         |                |
// | house_letter  | varchar(10)                | YES  |     |         |                |
// | floor         | varchar(10)                | YES  |     |         |                |
// | side_door     | varchar(10)                | YES  |     |         |                |
// | place_name    | varchar(40)                | YES  |     |         |                |
// | city          | varchar(70)                | YES  |     |         |                |
// | postal_number | varchar(32)                | YES  |     |         |                |
// | country_code  | char(2)                    | YES  |     | NULL    |                |
// | created_at    | timestamp                  | YES  |     | NULL    |                |
// | updated_at    | datetime                   | YES  |     | NULL    |                |
// +---------------+----------------------------+------+-----+---------+----------------+

function convertAddress (tbl_bruger, member_id, callback) {
  var postal_number = tbl_bruger.postnummer_dk !== null && tbl_bruger.postnummer_dk !== 0 ? tbl_bruger.postnummer_dk.toString()
    : tbl_bruger.postnummer !== '' ? tbl_bruger.postnummer
    : null;

  if (postal_number !== null &&
    (tbl_bruger.vejnavn !== null && tbl_bruger.vejnavn !== '') &&
    (tbl_bruger.bynavn !== null && tbl_bruger.bynavn !== '') &&
    (tbl_bruger.land !== null && tbl_bruger.land !== '')) {

    // TODO: Maybe we can use udland_flag?
    // TODO: How should we convert land eg. 'Canada'

    var address = {
      member_id: member_id,
      active: 1,
      type: 'billing',
      system_id: system_id_mdb_sync,
      road_name: tbl_bruger.vejnavn,
      house_number: tbl_bruger.husnummer,
      house_letter: tbl_bruger.husbogstav,
      floor: tbl_bruger.etage,
      side_door: tbl_bruger.sidedoer,
      place_name: tbl_bruger.stednavn,
      city: tbl_bruger.bynavn,
      postal_number: postal_number,
      country: tbl_bruger.land,
      country_code: null
    };

    userdb.insert('address', address, callback);
  } else {
    callback();
  }
}


// mysql> show columns from foreign_key;
// +------------+------------------+------+-----+---------+----------------+
// | Field      | Type             | Null | Key | Default | Extra          |
// +------------+------------------+------+-----+---------+----------------+
// | id         | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | system_id  | int(11) unsigned | NO   | MUL | NULL    |                |
// | member_id  | int(11) unsigned | NO   | MUL | NULL    |                |
// | system_key | varchar(255)     | YES  |     |         |                |
// +------------+------------------+------+-----+---------+----------------+

function convertForeignKey (tbl_bruger, member_id, callback) {
  var foreign_key = {
    system_id: system_id_mdb_sync,
    member_id: member_id,
    system_key: tbl_bruger.ekstern_id
  };

  userdb.insert('foreign_key', foreign_key, callback);
}



// mysql> show columns from interest_line;
// +-------------+---------------------+------+-----+-------------------+----------------+
// | Field       | Type                | Null | Key | Default           | Extra          |
// +-------------+---------------------+------+-----+-------------------+----------------+
// | id          | int(11) unsigned    | NO   | PRI | NULL              | auto_increment |
// | member_id   | int(11) unsigned    | NO   | MUL | NULL              |                |
// | interest_id | int(11) unsigned    | NO   | MUL | NULL              |                |
// | location_id | int(11) unsigned    | NO   | MUL | NULL              |                |
// | active      | tinyint(3) unsigned | YES  |     | 1                 |                |
// | created_at  | timestamp           | YES  |     | CURRENT_TIMESTAMP |                |
// | updated_at  | datetime            | YES  |     | NULL              |                |
// +-------------+---------------------+------+-----+-------------------+----------------+

module.exports.convertInteresseLinier = function (callback) {
  var done = 0;
  work();

  function work () {
    if (exiting) {
      console.log('Clean exit.');
      process.exit(0);
    }

    client.RPOP('tbl_interesse_linie', function (err, data) {
      if (err) throw err;
      
      if (data === null)
        return callback();

      var tbl_interesse_linie = JSON.parse(data);


      client.HGET('members', tbl_interesse_linie.user_id, function (err, member_id) {
        if (err) throw err;

        if (member_id === null) {
          work();
          return;
        }

        client.HGET('locations', tbl_interesse_linie.location_id, function (err, location_id) {
          if (err) throw err;

          if (location_id === null) {
            work();
            return;
          }

          client.HGET('interests', tbl_interesse_linie.interesse_id, function (err, interest_id) {
            if (err) throw err;

            if (interest_id === null) {
              work();
              return;
            }

            client.SISMEMBER('interest_line:' + member_id, interest_id, function (err, result) {
              if (err) throw err;


              if (result === 1) {
                work();
                return;
              } else {

                var interest_line = {
                  member_id: parseInt(member_id),
                  location_id: parseInt(location_id),
                  interest_id: parseInt(interest_id),
                  active: 1,
                  created_at: tbl_interesse_linie.oprettet
                };

                userdb.insert('interest_line', interest_line, function () {
                  client.SADD('interest_line:' + member_id, interest_id);
                  work();
                  return;
                });
              }
            });
          });
        });
      });
    });

    if (++done % 1000 === 0) {
      client.LLEN('tbl_interesse_linie', function (err, data) {
        console.log('Items to go from tbl_interesse_linie:', data, new Date().toString());
      });
    }
  }
};




// mysql> show columns from action_history;
// +----------------+---------------------+------+-----+---------+----------------+
// | Field          | Type                | Null | Key | Default | Extra          |
// +----------------+---------------------+------+-----+---------+----------------+
// | id             | int(11) unsigned    | NO   | PRI | NULL    | auto_increment |
// | member_id      | int(11) unsigned    | NO   | MUL | NULL    |                |
// | action_type_id | tinyint(3) unsigned | NO   | MUL | NULL    |                |
// | description    | varchar(255)        | YES  |     |         |                |
// | created_at     | timestamp           | YES  |     | NULL    |                |
// | info           | text                | YES  |     | NULL    |                |
// +----------------+---------------------+------+-----+---------+----------------+

module.exports.convertUserActions = function (callback) {
  var done = 0;
  work();

  function work () {
    if (exiting) {
      console.log('Clean exit.');
      process.exit(0);
    }
    
    client.RPOP('tbl_user_action', function (err, data) {

      if (data === null)
        return callback();

      var tbl_user_action = JSON.parse(data);

      var user_action_type_name = get_user_action_type_name(tbl_user_action.user_action_type_id);
      
      client.HGET('members', tbl_user_action.user_id, function (err, member_id) {
        if (member_id !== null) {

          client.HGET('action_types', user_action_type_name, 'id', function (err, action_type_id) {

            var action_history = {
              member_id: parseInt(member_id),
              action_type_id: parseInt(action_type_id),
              description: user_action_type_name + '(' + tbl_user_action.user_action_id + ')',
              created_at: tbl_user_action.oprettet,
              info: 'MDB Value: ' + tbl_user_action.value
            };

            // If the user action is a signoff, we might find user_feedback in tbl_user_afmelding.
            if ([2,4].indexOf(tbl_user_action.user_action_type_id) > -1) {
              mdb.select_all_from('tbl_user_afmelding WHERE user_id = ' + tbl_user_action.user_id + ' AND nyhedsbrev_id = ' + tbl_user_action.value, function (err, result) {
                if (result.rowCount > 0) {
                  action_history.info = result.rows[0].user_feedback;
                  userdb.insert('action_history', action_history);
                } else {
                  userdb.insert('action_history', action_history);
                }
              });
              
              work();
              return;

            } else {
              userdb.insert('action_history', action_history);
              
              work();
              return;
            }
          });
        } else {

          work();
          return;
        }
      });
    });

    if (++done % 1000 === 0) {
      client.LLEN('tbl_user_action', function (err, data) {
        console.log('Items to go from tbl_user_action:', data, new Date().toString());
      });
    }
  }
};

function get_user_action_type_name (user_action_type_id) {
  switch (user_action_type_id) {
    case 1:
      return 'Nyhedsbrev signup';
      break;
    case 2:
      return 'Nyhedsbrev_signoff';
      break;
    case 3:
      return 'Interesse signup';
      break;
    case 4:
      return 'Interesse signoff';
      break;
    case 5:
      return 'Doubleopt user entry';
      break;
    case 6:
      return 'Doubleopt accept';
      break;
  }
}





// mysql> show columns from phone_type;
// +-------+------------------+------+-----+---------+----------------+
// | Field | Type             | Null | Key | Default | Extra          |
// +-------+------------------+------+-----+---------+----------------+
// | id    | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | type  | varchar(20)      | NO   |     | NULL    |                |
// +-------+------------------+------+-----+---------+----------------+

function createOrFindTelefonPhoneType () {
  userdb.select_id_from('phone_type WHERE type="Telefon"', function (err, result) {
    if (result.length === 1) {
      phone_type_telefon = result[0].id;
      workerEmitter.emit('telefon_ready');
    } else {
      userdb.insert('phone_type', { type: 'Telefon' }, function (err, result) {
        phone_type_telefon = result.insertId;
        workerEmitter.emit('telefon_ready');
      });
    }
  });
}

function createOrFindMobilPhoneType () {
  userdb.select_id_from('phone_type WHERE type="Mobil"', function (err, result) {
    if (result.length === 1) {
      phone_type_mobil = result[0].id;
      workerEmitter.emit('mobil_ready');
    } else {
      userdb.insert('phone_type', { type: 'Mobil' }, function (err, result) {
        phone_type_mobil = result.insertId;
        workerEmitter.emit('mobil_ready');
      });
    }
  });
}


// mysql> show columns from system;
// +--------+------------------+------+-----+---------+----------------+
// | Field  | Type             | Null | Key | Default | Extra          |
// +--------+------------------+------+-----+---------+----------------+
// | id     | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | name   | varchar(255)     | NO   |     |         |                |
// | active | tinyint(4)       | NO   |     | 1       |                |
// +--------+------------------+------+-----+---------+----------------+

function createMdbSyncSystem () {
  userdb.select_id_from('system WHERE name="mdb_sync"', function (err, result) {
    if (result.length === 1) {
      system_id_mdb_sync = result[0].id;
      workerEmitter.emit('system_ready');
    } else {
      userdb.insert('system', { name: 'mdb_sync' }, function (err, result) {
        system_id_mdb_sync = result.insertId;
        workerEmitter.emit('system_ready');
      });
    }
  });
}






// module.exports.diffMembers = function (callback) {
//   var convertedMembers = 0,
//       start = Date.now(),
//       splitTime = start;

//   work();

//   function work () {
//     if (exiting) {
//       console.log('Clean exit.');
//       process.exit(0);
//     }

//     client.RPOP('tbl_bruger', function (err, bruger) {
//       if (err) throw err;

//       if (bruger === null)
//         return callback();
//       else {
//         var tbl_bruger = JSON.parse(bruger);
//         client.HEXISTS('members', tbl_bruger.user_id, function (err, exists) {
//           if (exists === 0) {
//             client.LPUSH('tbl_bruger_diff', bruger, function (err, result) {
//               work();
//             });
//           } else {
//             work();
//           }
//         });
//       }
//     });
//   }
// };



// module.exports.missingInMysql = function (callback) {
//   mdb.query('SELECT user_id FROM tbl_bruger', function (err, result) {
//     console.log('Got the ids from mdb', result.rowCount);
//     var done = 0,
//         count = result.rowCount,
//         mdb_ids = result.rows.map (function (tbl_bruger) {
//           return tbl_bruger.user_id;
//         });

//     console.log('MDB Ids count after map', mdb_ids.length);


//     userdb.query('SELECT mdb_user_id FROM member', function (err, result) {
//       console.log('Got the ids from userdb', result.length);
//       var userdb_ids = result.map(function (member) {
//         return member.mdb_user_id;
//       });

//       console.log('UserDB Ids count after map', userdb_ids.length);

//       mdb_ids.forEach(function (user_id) {

//         var existsInMySQL = userdb_ids.indexOf(user_id) > 0;

//         if (!existsInMySQL) {
//           client.SADD('smissing_mysql', user_id);
//         }

//         if (++done === count) {
//           callback();
//         }


//         if (done % 1000 === 0) {
//           console.log(done, 'done /', count);
//         }
//       });
//     });
//   });
// };

// module.exports.missingInRedis = function (callback) {
//   mdb.query('SELECT user_id FROM tbl_bruger', function (err, result) {
//     console.log('Got the ids from mdb', result.rowCount);
//     var done = 0,
//         count = result.rowCount,
//         mdb_ids = result.rows.map (function (tbl_bruger) {
//           return tbl_bruger.user_id;
//         });

//     console.log('MDB Ids count after map', mdb_ids.length);

//     mdb_ids.forEach(function (user_id) {

//       client.HEXISTS('members', user_id, function (err, exists) {

//         if (exists === 0) {
//           client.SADD('smissing_redis', user_id);
//         }

//         if (++done === count) {
//           callback();
//         }

//         if (done % 1000 === 0) {
//           console.log(done, 'done /', count);
//         }
//       });
//     });
//   });
// };




