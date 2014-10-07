/*jshint node: true */

'use strict';

var mdb = require('./mdb_client'),
    userdb = require('./userdb_client'),
    redis_helper = require('./redis_helper'),
    redis = require("redis"),
    client = redis.createClient();


SELECT count(user_id) FROM tbl_bruger;
SELECT count(user_id) FROM tbl_bruger WHERE email IS NOT NULL AND email != ''
SELECT count(user_id) FROM tbl_bruger WHERE telefon IS NOT NULL AND telefon != ''
SELECT count(user_id) FROM tbl_bruger WHERE mobil IS NOT NULL AND mobil != ''
SELECT count(user_id) FROM tbl_bruger WHERE ekstern_id IS NOT NULL AND ekstern_id != ''

SELECT count(*) FROM tbl_interesse_linie;


select count(*) from tbl_signup_nyhedsbrev inner join tbl_nyhedsbrev on tbl_nyhedsbrev.nyhedsbrev_id = tbl_signup_nyhedsbrev.nyhedsbrev_id AND permission = 0;
  count  
---------
 2339756
(1 row)

select count(*) from tbl_signup_nyhedsbrev inner join tbl_nyhedsbrev on tbl_nyhedsbrev.nyhedsbrev_id = tbl_signup_nyhedsbrev.nyhedsbrev_id AND permission = 1;
  count  
---------
 1005750
(1 row)


select user_id, foedselsaar, foedselsdato from tbl_bruger