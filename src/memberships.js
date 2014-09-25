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




module.exports.readSignupsIntoRedis = function (callback) {
  redis_helper.createListCopyFromMdb('tbl_signup_nyhedsbrev', 'tbl_signup_nyhedsbrev', callback);
};

module.exports.readSignoutsIntoRedis = function (callback) {
  redis_helper.createListCopyFromMdb('tbl_user_afmelding', 'tbl_user_afmelding', callback);
};

module.exports.readOptOutsIntoRedis = function (callback) {
  redis_helper.createListCopyFromMdb('tbl_mail_optout', 'tbl_mail_optout', callback);
};

module.exports.mapEmailsIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT member_id, id FROM email', 'emails', callback);
};


// mysql> show columns from subscription_member;
// +-----------------+---------------------+------+-----+---------+----------------+
// | Field           | Type                | Null | Key | Default | Extra          |
// +-----------------+---------------------+------+-----+---------+----------------+
// | id              | int(11) unsigned    | NO   | PRI | NULL    | auto_increment |
// | member_id       | int(11) unsigned    | NO   | MUL | NULL    |                |
// | email_id        | int(11) unsigned    | NO   | MUL | NULL    |                |
// | subscription_id | int(11) unsigned    | NO   | MUL | NULL    |                |
// | location_id     | int(11) unsigned    | NO   | MUL | NULL    |                |
// | active          | tinyint(3) unsigned | YES  |     | 1       |                |
// | joined          | datetime            | YES  |     | NULL    |                |
// | unjoined        | datetime            | YES  |     | NULL    |                |
// | unsub_reason_id | int(11) unsigned    | YES  | MUL | NULL    |                |
// +-----------------+---------------------+------+-----+---------+----------------+

// mysql> show columns from permission_member;
// +-----------------+---------------------+------+-----+---------+----------------+
// | Field           | Type                | Null | Key | Default | Extra          |
// +-----------------+---------------------+------+-----+---------+----------------+
// | id              | int(11)             | NO   | PRI | NULL    | auto_increment |
// | member_id       | int(11) unsigned    | NO   | MUL | NULL    |                |
// | email_id        | int(11) unsigned    | NO   | MUL | NULL    |                |
// | permission_id   | int(11) unsigned    | NO   | MUL | NULL    |                |
// | location_id     | int(11) unsigned    | NO   | MUL | NULL    |                |
// | active          | tinyint(3) unsigned | YES  |     | 1       |                |
// | joined          | datetime            | YES  |     | NULL    |                |
// | unjoined        | datetime            | YES  |     | NULL    |                |
// | unsub_reason_id | int(11) unsigned    | YES  | MUL | NULL    |                |
// +-----------------+---------------------+------+-----+---------+----------------+

module.exports.convertSignups = function (callback) {
  var done = 0;
  work();

  function work () {
    client.RPOP('tbl_signup_nyhedsbrev', function (err, data) {

      if (data === null)
        return callback();

      var tbl_signup_nyhedsbrev = JSON.parse(data);

      client.HGET('members', tbl_signup_nyhedsbrev.user_id, function (err, member_id) {
        if (member_id !== null) {

          client.HGET('emails', member_id, function (err, email_id) {
            client.HGET('locations', tbl_signup_nyhedsbrev.location_id, function (err, location_id) {

              var membership = {
                member_id: parseInt(member_id),
                email_id: parseInt(email_id),
                location_id: parseInt(location_id),
                active: parseInt(tbl_signup_nyhedsbrev.signup_flag), //signup_flag '1' eller '0'
                joined: tbl_signup_nyhedsbrev.signup_dato,
                unjoined: tbl_signup_nyhedsbrev.signout_dato
                // unsub_reason_id: 'TODO'
              };

              client.HEXISTS('subscriptions', tbl_signup_nyhedsbrev.nyhedsbrev_id, function (err, reply) {
                var is_subscription = reply === 1,
                    hash = is_subscription ? 'subscriptions' : 'permissions';
                
                client.HGET(hash, tbl_signup_nyhedsbrev.nyhedsbrev_id, function (err, result) {
                  if (is_subscription) {
                    var subscription_id = parseInt(result);

                    selectConvertedSubscriptionMember(member_id, email_id, function (err, result) {

                      if (result.length === 0) {
                        membership.subscription_id = subscription_id;
                        userdb.insert('subscription_member', membership);
                      }

                      work();
                      return;
                    });

                  } else {
                    var permission_id = parseInt(result);

                    selectConvertedPermissionMember(member_id, email_id, function (err, result) {

                      if (result.length === 0) {
                        membership.permission_id = permission_id;
                        userdb.insert('permission_member', membership);
                      }

                      work();
                      return;
                    });
                  }
                });
              });
            });
          });
        } else {
          work();
          return;
        }
      });
    });

    if (++done % 1000 === 0) {
      client.LLEN('tbl_signup_nyhedsbrev', function (err, data) {
        console.log('Items to go from tbl_signup_nyhedsbrev:', data, new Date().toString());
      });
    }
  }
}



// mysql> show columns from unsub_reason;
// +--------------------+------------------+------+-----+---------+----------------+
// | Field              | Type             | Null | Key | Default | Extra          |
// +--------------------+------------------+------+-----+---------+----------------+
// | id                 | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
// | reason_type_id     | int(11) unsigned | NO   | MUL | NULL    |                |
// | custom_reason_text | text             | NO   |     | NULL    |                |
// +--------------------+------------------+------+-----+---------+----------------+

// tbl_user_afmelding: ===
//  user_afmelding_id | user_id | nyhedsbrev_id | location_id |                                                                               user_feedback                                                                                |          oprettet          
// -------------------+---------+---------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------
//              49851 | 2475353 |           108 |           1 | Indholdet i jeres mails er ikke relevant for mig                                                                                                                           | 2013-08-04 20:39:07.68253
//              16085 | 2060899 |           108 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-09-09 19:11:52.545999
//                  4 |  567421 |           108 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-15 14:50:20.959231
//                  5 |  567421 |           108 |           1 | Andet: Test                                                                                                                                                                | 2012-03-15 14:51:14.978275
//                  6 |  567421 |           130 |           1 | Jeg har deltaget i en konkurrence, men ønsker ikke at være tilmeldt                                                                                                        | 2012-03-15 14:51:49.830845
//                  7 |  567421 |           108 |           1 | I sender for mange mails                                                                                                                                                   | 2012-03-15 14:52:09.342492
//                  8 |  567421 |           130 |           1 | Andet: Anden test                                                                                                                                                          | 2012-03-15 14:52:09.342246
//                  9 |  567421 |           130 |           1 | Andet: Virker det?                                                                                                                                                         | 2012-03-15 14:59:02.341118
//                 10 | 1290229 |           108 |           1 | Andet: får på en anden mailadresse                                                                                                                                         | 2012-03-16 08:49:41.569386
//                 11 |  855131 |           108 |           1 | Indholdet i jeres mails er ikke relevant for mig                                                                                                                           | 2012-03-16 09:23:02.600704
//                 12 | 2013371 |           108 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 09:39:36.522965
//                 13 | 2013371 |           130 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 09:39:36.541706
//                 14 | 1444107 |           130 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 09:43:29.067228
//                 15 |   68549 |           130 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 09:48:03.786537
//                 16 |  964556 |           108 |           1 | Jeg er blevet tilmeldt ved en fejl                                                                                                                                         | 2012-03-16 09:55:18.990246
//                 17 |  964556 |           130 |           1 | Jeg er blevet tilmeldt ved en fejl                                                                                                                                         | 2012-03-16 09:55:33.173338
//                 18 |  269560 |           108 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 10:08:37.31642
//                 19 |  269560 |           108 |           1 | Jeg modtager for mange mails generelt                                                                                                                                      | 2012-03-16 10:08:51.443173
//                 20 | 2010998 |           108 |           1 | Jeg er blevet tilmeldt ved en fejl                         

module.exports.convertSignouts = function (callback) {
  var done = 0;
  work();

  function work () {
    client.RPOP('tbl_user_afmelding', function (err, data) {

      if (data === null)
        return callback();

      var tbl_user_afmelding = JSON.parse(data);

      // Getting the new (converted) member id from redis
      client.HGET('members', tbl_user_afmelding.user_id, function (err, member_id) {
        // If the member is not converted, we don't go any further
        if (member_id !== null) {

          // Finding the members email id - at the time of conversion, each member has max one email
          client.HGET('emails', member_id, function (err, email_id) {
            // Finding the converted location id from redis
            //client.HGET('locations', tbl_user_afmelding.location_id, function (err, location_id) {
              // Testing if the signout is a newsletter or a permission by checking if the id exists
              client.HEXISTS('subscriptions', tbl_user_afmelding.nyhedsbrev_id, function (err, exists) {
                
                // The signout is for a subscription, so we need to update the corresponding subscription member
                if (exists === 1) {
                  // Finding the converted subscription id from redis
                  client.HGET('subscriptions', tbl_user_afmelding.nyhedsbrev_id, function (err, subscription_id) {
                
                    // Finding the converted signup that is related to this signout
                    selectConvertedSubscriptionMember(member_id, email_id, function (err, result) {
                      // Only if the signup has been converted, we convert the signout
                      if (result.length === 1) {
                        var subscription_member = result[0];

                        // Inserting the reason for unsubscribing and updating the subscription membership in the callback
                        insertUnsubReason(tbl_user_afmelding, function (err, result) {
                          updateConvertedSubscriptionMember(subscription_member.id, result.insertId);
                        });

                        work();
                        return;
                      }
                    });
                  });

                // The signout is for a permission, so we need to update the corresponding permission member
                } else {
                  // Finding the converted permission id from redis
                  client.HGET('permissions', tbl_user_afmelding.nyhedsbrev_id, function (err, permission_id) {
                    
                    // Finding the converted permission signup that is related to this signout
                    selectConvertedPermissionMember(member_id, email_id, function (err, result) {
                      if (result.length === 1) {
                        var permission_member = result[0];

                        // Inserting the reason for unsubscribing and updating the permission membership in the callback
                        insertUnsubReason(tbl_user_afmelding, function (err, result) {
                          updateConvertedPermissionMember(permission_member.id, result.insertId);
                        });

                        work();
                        return;
                      }
                    })
                  });
                }

                // TODO:
                // => action_history - Brug info-kolonnen.

                // var action_history = {
                //   member_id: member_id,
                //   action_type_id: id,
                //   description: user_action_type_name + '(' + user_action_type.user_action_id + ')',
                //   created_at: user_action_type.oprettet,
                //   info: 'TODO'
                // }

                // userdb.insert('action_history', action_history, errHandler);

 
              });
            //});
          });
        } else {
          work();
        }
      });
    });

    if (++done % 1000 === 0) {
      client.LLEN('tbl_user_afmelding', function (err, data) {
        console.log('Items to go from tbl_user_afmelding:', data, new Date().toString());
      });
    }
  }
}


function insertUnsubReason (tbl_user_afmelding, callback) {
  var unsub_reason = {};

  unsub_reason.custom_reason_text = tbl_user_afmelding.user_feedback.indexOf('Andet:') === 0 ?
     tbl_user_afmelding.user_feedback.substring(7) : null;

  var reason = tbl_user_afmelding.user_feedback.indexOf('Andet:') === 0 ?
    'Andet' : tbl_user_afmelding.user_feedback;
    
  client.HGET('reason_types', reason, function (err, reason_type_id) {
    unsub_reason.reason_type_id = reason_type_id;
    userdb.insert('unsub_reason', unsub_reason, callback);
  });
}

function updateConvertedSubscriptionMember (membership_id, unsub_reason_id) {
  updateConvertedMembership('subscription_member', membership_id, unsub_reason_id);
}

function updateConvertedPermissionMember (membership_id, unsub_reason_id) {
  updateConvertedMembership('permission_member', membership_id, unsub_reason_id);
}

function updateConvertedMembership(membershipTable, membership_id, unsub_reason_id) {
  var membership = {
    id: membership_id,
    unsub_reason_id: unsub_reason_id
  }

  userdb.update(membershipTable, membership);
}

function selectConvertedSubscriptionMember (member_id, email_id, callback) {
selectConvertedMembership('subscription_member', member_id, email_id, callback);
}

function selectConvertedPermissionMember (member_id, email_id, callback) {
  selectConvertedMembership('permission_member', member_id, email_id, callback);
}

function selectConvertedMembership (tableName, member_id, email_id, callback) {
  userdb.query('SELECT id FROM ' + tableName +
    ' WHERE member_id = ' + member_id +
    ' AND email_id = ' + email_id,
    //' AND location_id = ' + location_id,
    function (err, result) {
      if (err) throw err;

    callback(err, result);
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

// TODO: test
module.exports.convertOptOuts = function (callback) {
  client.RPOP('tbl_mail_optout', function (err, data) {

    if (data === null)
      return callback();

    var tbl_mail_optout = JSON.parse(data);


    //mdb.query('SELECT mail_optout_id, insert_ts FROM tbl_mail_optout WHERE email =\'' + tbl_bruger.email + '\'', function (err, result) {
    userdb.query('SELECT id FROM email WHERE email_address =\'' + tbl_mail_optout.email + '\'', function (err, result) {
      if (result.length === 0) throw new Error('Email ' + tbl_mail_optout.email + ' not found');

      var email_id = result[0].id;
      var opt_outs = {
        email_id: email_id,
        timestamp: tbl_mail_optout.insert_ts
      }

      userdb.insert('opt_outs', opt_outs);
    });
  });
};

