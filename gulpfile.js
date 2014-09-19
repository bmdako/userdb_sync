var gulp = require('gulp'),
    members = require('./src/members'),
    memberships = require('./src/memberships'),
    publishers = require('./src/publishers'),
    systemdata = require('./src/systemdata');

gulp.task('default', [
  'convertLocations',
  'convertPublishers',
  'convertSubscriptions' ], function () {
  // place code for your default task here
});

gulp.task('convertLocations', ['readLocationMappingIntoRedis'], function (callback) {
  systemdata.convertLocations(callback);
});

gulp.task('convertReasonTypes', ['readReasonTypeMappingIntoRedis'], function (callback) {
  systemdata.convertReasonTypes(callback);
});

gulp.task('convertActionTypes', ['readActionTypeMappingIntoRedis'], function (callback) {
  systemdata.convertActionTypes(callback);
});

gulp.task('convertInterests', ['readInterestMappingIntoRedis'], function (callback) {
  systemdata.convertInterests(callback);
});

gulp.task('convertInterestParents', function (callback) {
  systemdata.convertInterestParents(callback);
});

gulp.task('convertPermissions', ['readPermissionMappingIntoRedis'], function (callback) {
  systemdata.convertPermissions(callback);
});

gulp.task('convertPublishers', ['readPublisherMappingIntoRedis'], function (callback) {
  publishers.convertPublishers(callback);
});

gulp.task('convertSubscriptions', ['readSubscriptionMappingIntoRedis'], function (callback) {
  publishers.convertSubscriptions(callback);
});

gulp.task('convertMembers', ['readMembersUserIdMappingIntoRedis', 'readUserIdIntoRedis'], function (callback) {
  members.convertMembers(callback);
});

gulp.task('convertUserActions', function (callback) {
  members.convertUserActions(callback);
});

gulp.task('convertSignups', function (callback) {
  memberships.convertSignups(callback);
});

gulp.task('convertSignouts', function (callback) {
  memberships.convertSignouts(callback);
});


// Bruger import-list
gulp.task('readUserIdIntoRedis', function (callback) {
  members.readUserIdIntoRedis(callback);
});

// Signups cache
gulp.task('readSubscriptionMembersIntoRedis', function (callback) {
  memberships.readSubscriptionMembersIntoRedis(callback);
});

// Signouts cache
gulp.task('readUnsubscriptionIntoRedis', function (callback) {
  memberships.readUnsubscriptionIntoRedis(callback);
});


// To be used for importing signups (subscription_member and permission_member), signouts and user_actions.
gulp.task('readMembersUserIdMappingIntoRedis', function (callback) {
  members.readMembersUserIdMappingIntoRedis(callback);
});



// Below are task tp prepare supporting tables. Their mdb_id and usersdb_id are mapped into a hash in redis.
// This way we can easily look up if the row has been imported and what their new id is.
gulp.task('prepare', [
  'readLocationMappingIntoRedis',
  'readSubscriptionMappingIntoRedis',
  'readPermissionMappingIntoRedis',
  'readInterestMappingIntoRedis',
  'readPublisherMappingIntoRedis',
  'readActionTypeMappingIntoRedis',
  'readReasonTypeMappingIntoRedis' ], function () {
  // place code for your default task here
});


gulp.task('readLocationMappingIntoRedis', function (callback) {
  systemdata.readLocationMappingIntoRedis(callback);
});

gulp.task('readSubscriptionMappingIntoRedis', function (callback) {
  publishers.readSubscriptionMappingIntoRedis(callback);
});

gulp.task('readPublisherMappingIntoRedis', function (callback) {
  publishers.readPublisherMappingIntoRedis(callback);
});

gulp.task('readPermissionMappingIntoRedis', function (callback) {
  systemdata.readPermissionMappingIntoRedis(callback);
});

gulp.task('readInterestMappingIntoRedis', function (callback) {
  systemdata.readInterestMappingIntoRedis(callback);
});

gulp.task('readActionTypeMappingIntoRedis', function (callback) {
  systemdata.readActionTypeMappingIntoRedis(callback);
});

gulp.task('readReasonTypeMappingIntoRedis', function (callback) {
  systemdata.readReasonTypeMappingIntoRedis(callback);
});
