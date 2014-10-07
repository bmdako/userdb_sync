userdb_sync
===========


HASH locations (fra UserDB location)
  mdb_location_id : id

HASH permissions (fra UserDB permission)
  mdb_nyhedsbrev_id : id

HASH interests (fra UserDB interest)
  mdb_interesse_id : id

HASH action_types (fra UserDB action_type)
  description : id

HASH reason_types (fra UserDB reason_type)
  text : id




HASH publishers (fra UserDB publisher - bliver brugt til at sikre vi kan importere igen og igen)
  mdb_publisher_id : id

HASH subscriptions (fra UserDB subscription - bliver brugt til at sikre vi kan importere igen og igen)
  mdb_nyhedsbrev_id : id

LIST tbl_bruger (fra MDB)
  json.stringified

LIST tbl_interesse_linie (fra MDB)
  json.stringified

LIST tbl_user_action (fra MDB)
  json.stringified

HASH members (fra UserDB member)
  mdb_user_id : id

SET interest_line
  member_id : interest_id interest_id interest_id interest_id



SET smembers
  id id id id id

SET sbrugere
  user_id user_id user_id user_id

SET smissing
  user_id user_id user_id user_id


LIST tbl_signup_nyhedsbrev
  json.stringified

LIST tbl_user_afmelding
  json.stringified

LIST tbl_mail_optout
  json.stringified


HASH emails (fra UserDB email)
  member_id: id

HASH email_ids (fra UserDB email)
  email_address : id

SET subscription_member:member_id
   subscription_id subscription_id subscription_id

SET permission_member:member_id
   permission_id permission_id permission_id

SET opt_outs
  email_id email_id email_id email_id email_id