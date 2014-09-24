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


module.exports.mapPublisherIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_publisher_id, id FROM publisher', 'publishers', callback);
};


module.exports.mapSubscriptionIntoRedis = function (callback) {
  redis_helper.createHashMappingFromUserdb('SELECT mdb_nyhedsbrev_id, id FROM subscription', 'subscriptions', callback);
};


// mysql> show columns from publisher;
// +------------------+------------------+------+-----+-------------------+----------------+
// | Field            | Type             | Null | Key | Default           | Extra          |
// +------------------+------------------+------+-----+-------------------+----------------+
// | id               | int(11) unsigned | NO   | PRI | NULL              | auto_increment |
// | name             | varchar(255)     | YES  |     |                   |                |
// | display_text     | varchar(255)     | YES  |     |                   |                |
// | from_email       | varchar(255)     | YES  |     |                   |                |
// | from_name        | varchar(255)     | YES  |     |                   |                |
// | url_picture_top  | varchar(255)     | YES  |     |                   |                |
// | active           | tinyint(4)       | NO   |     | 1                 |                |
// | url              | varchar(255)     | YES  |     |                   |                |
// | created_at       | timestamp        | YES  |     | CURRENT_TIMESTAMP |                |
// | mdb_publisher_id | int(11)          | YES  |     | NULL              |                |
// +------------------+------------------+------+-----+-------------------+----------------+

module.exports.convertPublishers = function (callback) {
  mdb.select_all_from('tbl_publisher', function (err, result) {
    
    if (result.rowCount === 0)
      if (callback !== undefined && typeof callback === 'function')
        return callback();

    var count = result.rowCount,
        done = 0;

    result.rows.forEach(function (tbl_publisher) {

      client.HEXISTS('publishers', tbl_publisher.publisher_id, function (err, exists) {
        if (exists === 0) {
          // publisher_sendfrom eg. Berlingske Media <berlingskemedia@berlingskemedia-mail.dk>
          var i = tbl_publisher.publisher_sendfrom.indexOf('<');
          var j = tbl_publisher.publisher_sendfrom.indexOf('>');

          // TODO: Hvad skal vi gøre med?:
            // publisher_sort
            // alien_newsletter_list
            // publisher_interesser
            // wheel_title

          var publisher = {
            name: tbl_publisher.publisher_navn,
            display_text: tbl_publisher.publisher_brugernavn,
            from_email: tbl_publisher.publisher_sendfrom.substring(i + 1, j),
            from_name: tbl_publisher.publisher_sendfrom.substring(0,i).trim(),
            url_picture_top: tbl_publisher.publisher_toppic,
            active: tbl_publisher.enabled,
            url: tbl_publisher.publisher_url,
            mdb_publisher_id: tbl_publisher.publisher_id
          }

          userdb.insert('publisher', publisher, function (err, result) {
            client.HSET('publishers', tbl_publisher.publisher_id, result.insertId);
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


// mysql> show columns from subscription;
// +-------------------+---------------------+------+-----+---------+----------------+
// | Field             | Type                | Null | Key | Default | Extra          |
// +-------------------+---------------------+------+-----+---------+----------------+
// | id                | int(11) unsigned    | NO   | PRI | NULL    | auto_increment |
// | publisher_id      | int(11) unsigned    | NO   | MUL | NULL    |                |
// | name              | varchar(255)        | YES  |     | NULL    |                |
// | active            | tinyint(3) unsigned | YES  |     | 1       |                |
// | display_text      | varchar(255)        | YES  |     | NULL    |                |
// | description       | text                | YES  |     | NULL    |                |
// | mdb_nyhedsbrev_id | int(11)             | YES  |     | NULL    |                |
// +-------------------+---------------------+------+-----+---------+----------------+

module.exports.convertSubscriptions = function (callback) {
  mdb.select_all_from('tbl_nyhedsbrev WHERE permission = 0', function (err, result) {

    if (result.rowCount === 0)
      if (callback !== undefined && typeof callback === 'function')
        return callback();

    var count = result.rowCount,
        done = 0;
    
    result.rows.forEach(function (tbl_nyhedsbrev) {

      client.HEXISTS('subscriptions', tbl_nyhedsbrev.nyhedsbrev_id, function (err, exists) {
        if (exists === 0) {
          // TODO: Hvad skal vi gøre med?:
            // nyhedsbrev_tag  | character varying(255)
            // eksternt_id     | character varying(255) 
            // tidspunkt       | character varying(255)
            // markeret        | smallint
            // highlight       | smallint
            // nyt             | smallint - måske relevant
            // hidden          | smallint - relevant

          var subscription = {
            name: tbl_nyhedsbrev.nyhedsbrev_navn,
            active: tbl_nyhedsbrev.enabled,
            display_text: tbl_nyhedsbrev.nyhedsbrev_navn,
            description: tbl_nyhedsbrev.indhold,
            mdb_nyhedsbrev_id: tbl_nyhedsbrev.nyhedsbrev_id
          }

          // if (tbl_nyhedsbrev.publisher_id === null) {

          //   subscription.publisher_id = berlingske_media_publisher_id;
          //   console.log('1', subscription);
          //   insert(subscription);

          // } else {

          client.HGET('publishers', tbl_nyhedsbrev.publisher_id, function (err, publisher_id) {

            if (tbl_nyhedsbrev.publisher_id === null) {
              console.log('WE GOT ONE', publisher_id);
            }

            // if (publisher_id === null) {
            //   // If we still cannot find publisher - because it's not in the MDB database.
            //   // f.eks. publisher 43 not found for nyhedsbrev_id 272
            //   subscription.publisher_id = berlingske_media_publisher_id;
            //   console.log('2', subscription);
            //   insert(subscription);
            // } else {
              subscription.publisher_id = parseInt(publisher_id);
              console.log('3', subscription);
              insert(subscription);
            // }

          });
          // }
        }
      });

      ++done;
      if (done === count)
        if (callback !== undefined && typeof callback === 'function')
          callback();
    });
  });

  function insert(subscription) {
    if (subscription.publisher_id === null || isNaN(subscription.publisher_id)) {
      findBerlingskeMediaPublisher(function (id) {
        subscription.publisher_id = id;
        insert(subscription);
      });
    } else {
      userdb.insert('subscription', subscription, function (err, result) {
        client.HSET('subscriptions', subscription.mdb_nyhedsbrev_id, result.insertId);
      });
    }
  }
};



function findBerlingskeMediaPublisher (callback) {
  userdb.select_id_from('publisher WHERE name="Berlingske Media"', function (err, result) {
    if (result.length === 1) {
      callback(result[0].id);
      // berlingske_media_publisher_id = result[0].id;
      // workerEmitter.emit('publisher_ready');
    } else {
      throw new Error('Publisher Berlingske Media not yet converted.')
    }
  });
}
