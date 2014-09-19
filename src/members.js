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
    // error_queue = 'convert_error',
    // skipped_queue = 'convert_skipped',


var phone_type_telefon = 1;
var phone_type_mobil = 2;
var system_id_mdb_sync = 1;


function time(start, label) {
  // console.log(label, 'time', (Date.now() - start) / 1000);
}

workerEmitter.on('get_ready', createOrFindTelefonPhoneType);
workerEmitter.on('telefon_ready', createOrFindMobilPhoneType);
workerEmitter.on('mobil_ready', createMdbSyncSystem);
workerEmitter.on('system_ready', function () {
  workerEmitter.emit('ready');
});



workerEmitter.on('findUser', findUser);
workerEmitter.on('findUser_done', convertMember);
workerEmitter.on('convertMember_done', insertIntoRedisMemberHashMapping);
workerEmitter.on('convertMember_done', convertEmail);
//workerEmitter.on('convertEmail_done', convertOptOuts);
workerEmitter.on('convertMember_done', convertTelefon);
workerEmitter.on('convertMember_done', convertMobil);
workerEmitter.on('convertMember_done', convertAddress);
workerEmitter.on('convertMember_done', convertForeignKey);
workerEmitter.on('convertMember_done', convertInteresseLinier);
workerEmitter.on('convertMembers_done', countUsers)
workerEmitter.on('convertInteresseLinier_done', function () {
  workerEmitter.emit('ready');
});


module.exports.readUserIdIntoRedis = function (callback) {
  client.DEL('brugere', 'brugere_done', function (err, result) {
    mdb.query('SELECT user_id FROM tbl_bruger ORDER BY user_id', function (err, result) {
      if (err) throw err;

      var count = result.rows.length,
          pushed = 0;

      result.rows.forEach(function (tbl_bruger) {
        client.LPUSH('brugere', tbl_bruger.user_id);
        ++pushed;
        if (pushed === count) {
          callback();
        }
      });
    });
  });
};

module.exports.readMembersUserIdMappingIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_user_id, id FROM member', 'members', callback);
}


module.exports.convertMembers = function (callback) {
  workerEmitter.on('ready', function () {
    workerEmitter.emit('findUser');
  });

  workerEmitter.on('findUser_empty', function () {
    // The callback might be the callback from gulp, so we fire of the callback to let gulp know the task is complete.
    if (callback !== undefined && typeof callback === 'function')
      callback();
  });

  // Starting the whole thing!
  workerEmitter.emit('get_ready');
}


function findUser () {
  client.RPOPLPUSH('brugere', 'brugere_done', function (err, task) {
    if (err) throw err;

    // List is empty
    if (task === null)
      workerEmitter.emit('findUser_empty');
    else
      workerEmitter.emit('findUser_done', task);
  });
}


var convertedMembers = 0;
function countUsers () {
  if (++convertedMembers % 1000 === 0) {
    console.log('Members converted: ', convertedMembers);
    client.LLEN('brugere', function (err, length) {
      console.log('Members left:', length);
    });
  }
}


function insertIntoRedisMemberHashMapping (tbl_bruger, member_id) {
  client.HSET('members', tbl_bruger.user_id, member_id);
}


function convertMember (user_id) {
  var start = Date.now();

  client.HEXISTS('members', user_id, function (err, result) {
    if (result === 0) {
      mdb.query('SELECT * FROM tbl_bruger WHERE user_id = ' + user_id, function (err, result) {
        var tbl_bruger = result.rows[0];

        time(start, 'convertMember - select tbl_bruger done  ' + user_id);

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
        }

        userdb.insert('member', member, function (err, result) {
          var member_id = result.insertId;

          workerEmitter.emit('convertMember_done', tbl_bruger, result.insertId);
          time(start, 'convertMember');
        });
      });
    }
  });
};


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

function convertEmail (tbl_bruger, member_id) {
  var start = Date.now();

  var email = {
    member_id: member_id,
    email_address: tbl_bruger.email,
    system_id: system_id_mdb_sync,
    active: 1
  };

  userdb.insert( 'email', email, function (err, result) {
    time(start, 'convertEmail');
    workerEmitter.emit('convertEmail_done', tbl_bruger, member_id, result.insertId);
  });
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

function convertTelefon (tbl_bruger, member_id) {
  var start = Date.now();

  if (tbl_bruger.telefon !== null && tbl_bruger.telefon !== '') {

    var telefon = {
      member_id: member_id,
      type_id: phone_type_telefon,
      system_id: system_id_mdb_sync,
      number: tbl_bruger.telefon,
      status: 1
    };

    userdb.insert('phone', telefon, function (err, result) {
      time(start, 'convertTelefon time');
      workerEmitter.emit('convertTelefon_done');
    });
  }
}

function convertMobil (tbl_bruger, member_id) {
  var start = Date.now();

  if (tbl_bruger.mobil !== null && tbl_bruger.mobil !== '') {

    var mobil = {
      member_id: member_id,
      type_id: phone_type_mobil,
      system_id: system_id_mdb_sync,
      number: tbl_bruger.mobil,
      status: 1
    }

    userdb.insert('phone', mobil, function (err, result) {
      time(start, 'convertMobil');
      workerEmitter.emit('convertMobil_done');
    });
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

function convertAddress (tbl_bruger, member_id) {
  var start = Date.now();

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

    userdb.insert('address', address, function (err, result) {
      time(start, 'convertAddress');
      workerEmitter.emit('convertAddress_done');
    });
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

function convertForeignKey (tbl_bruger, member_id) {
  var start = Date.now();

  var foreign_key = {
    system_id: system_id_mdb_sync,
    member_id: member_id,
    system_key: tbl_bruger.ekstern_id
  };

  userdb.insert('foreign_key', foreign_key, function (err, result) {
    time(start, 'convertForeignKey');
    workerEmitter.emit('convertForeignKey_done');
  });
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

function convertInteresseLinier (tbl_bruger, member_id) {
  var start = Date.now();

  mdb.query('SELECT interesse_id, location_id, oprettet FROM tbl_interesse_linie WHERE user_id = ' + tbl_bruger.user_id, function (err, result) {
    var count = result.rowCount,
        done = 0;

    if (count === 0)
      workerEmitter.emit('convertInteresseLinier_done');

    result.rows.forEach(function (tbl_interesse_linie) {
      client.HGET('interests', tbl_interesse_linie.interesse_id, function (err, interest_id) {
        client.HGET('locations', tbl_interesse_linie.location_id, function (err, location_id) {

          var interest_line = {
            member_id: member_id,
            interest_id: interest_id,
            location_id: location_id,
            active: 1,
            created_at: tbl_interesse_linie.oprettet
          }

          userdb.insert('interest_line', interest_line, function (err, result) {

            time(start, 'convertInteresseLinier - insert interest_line');

            ++done;
            if (done === count)
              workerEmitter.emit('convertInteresseLinier_done');
          });
        });
      });
    })
  });
}


// mysql> show columns from opt_out_desc;
// +-------------+------------------+------+-----+---------+----------------+
// | Field       | Type             | Null | Key | Default | Extra          |
// +-------------+------------------+------+-----+---------+----------------+
// | id          | int(10) unsigned | NO   | PRI | NULL    | auto_increment |
// | description | varchar(255)     | YES  |     | NULL    |                |
// +-------------+------------------+------+-----+---------+----------------+

// mysql> show columns from opt_outs;
// +-----------+------------------+------+-----+-------------------+----------------+
// | Field     | Type             | Null | Key | Default           | Extra          |
// +-----------+------------------+------+-----+-------------------+----------------+
// | id        | int(10) unsigned | NO   | PRI | NULL              | auto_increment |
// | email_id  | int(10) unsigned | NO   | MUL | NULL              |                |
// | timestamp | timestamp        | YES  |     | CURRENT_TIMESTAMP |                |
// +-----------+------------------+------+-----+-------------------+----------------+


//  mail_optout_id |               email               | type_id |         insert_ts          
// ----------------+-----------------------------------+---------+----------------------------
//            3140 | %email%                           |       1 | 2011-01-14 14:39:40.597789
//            1650 | henrik@adnuvo.com                 |       1 | 2010-11-10 13:03:03.508472
//            1651 | spe@berlingskemedia.dk            |       1 | 2010-11-09 17:20:57.548707
//           48520 | lhbj@os.dk                        |       7 | 2013-07-12 10:25:16.727443
//           23680 | annamariekjohansen@anarki.dk      |       2 | 2011-11-22 09:45:35.240032
//            1654 | hg@pilgrim.dk                     |       1 | 2010-11-29 08:33:12.566904
//            1655 | kres@kliniksalomonsen.dk          |       1 | 2010-11-29 08:34:20.175984
//            1656 | josefineforsgren@gmail.com        |       1 | 2010-11-29 08:37:24.819349
//            1657 | l_hauge@hotmail.com               |       1 | 2010-11-29 08:47:25.599159
//            1658 | liselotte.ferdinandsen@tmj.dk     |       1 | 2010-11-29 08:48:35.922237
//            1659 | iq@lite.dk                        |       1 | 2010-11-29 08:49:01.650809
//            1660 | info@currivie.dk                  |       1 | 2010-11-29 08:51:13.889021
//            1661 | ldrewsen@gmail.com                |       1 | 2010-11-29 08:52:59.136212

//  type_id |                      type_desc                      
// ---------+-----------------------------------------------------
//        1 | Fordelsmail
//        2 | Opdateringskampagne
//        3 | Berlingske servicemails
//        4 | BT servicemails
//        5 | Århus Stiftstidende servicemails
//        6 | AOK servicemails
//        7 | Servicemails (alle publikationer)
//        8 | Spørgeskema undersøgelser fra Medieanalyse, CRM mv.
//       14 | Berlingske Business Direct mails
//       15 | Ønsker ikke tilbud fra BT
//       16 | Ønsker ikke tilbud fra Berlingske
//       17 | Rejsemagasinet escape servicemails
//       18 | Kids News servicemail
// (13 rows)

function convertOptOuts (tbl_bruger, member_id, email_id) {
  var start = Date.now();

  mdb.query('SELECT mail_optout_id, insert_ts FROM tbl_mail_optout WHERE email =\'' + tbl_bruger.email + '\'', function (err, result) {

    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_mail_optout) {

      var opt_outs = {
        email_id: email_id,
        timestamp: tbl_mail_optout.insert_ts
      }

      userdb.insert('opt_outs', opt_outs, function (err, result) {
        time(start, 'convertOptOuts');

        ++done;
        if (done === count)
          workerEmitter.emit('convertOptOuts_done');
      });
    });
  });
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

module.exports.convertUserActions = function (callback) { // tbl_bruger, member_id
  var start = Date.now();

  // TODO
  mdb.select_all_from('tbl_user_action WHERE user_id = ' + tbl_bruger.user_id, function (err, result) {
    time(start, 'convertUserActions - select_all_from ' + tbl_bruger.user_id);
    result.rows.forEach(function(user_action) {

      var user_action_type_name = get_user_action_type_name(user_action.user_action_type_id);

      client.HGET('action_types', user_action_type_name, 'id', function (err, action_type_id) {
        time(start, 'convertUserActions - HGET action_type');

        var action_history = {
          member_id: member_id,
          action_type_id: action_type_id,
          description: user_action_type_name + '(' + user_action.user_action_id + ')',
          created_at: user_action.oprettet,
          info: 'MDB Value: ' + user_action.value
        }

        // If the user action is a signoff, we might find user_feedback in tbl_user_afmelding.
        if ([2,4].indexOf(user_action.user_action_type_id) > -1) {
          mdb.select_all_from('tbl_user_afmelding WHERE user_id = ' + tbl_bruger.user_id + ' AND nyhedsbrev_id = ' + user_action.value, function (err, result) {
            if (result.rowCount > 0) {
              action_history.info = result.rows[0].user_feedback;
              userdb.insert('action_history', action_history, function (err, result) {
                time(start, 'convertUserActions - insert 1');
              });
            } else {
              userdb.insert('action_history', action_history, function (err, result) {
                time(start, 'convertUserActions - insert 2');
              });
            }
          }); 
        } else {
          userdb.insert('action_history', action_history, function (err, result) {
            time(start, 'convertUserActions - insert 3');
          });
        }
      });
    });
  });
}

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

