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


gulp.task('systemdata', []);

gulp.task('convertLocations', ['readLocationMappingIntoRedis'], function (callback) {
  systemdata.convertLocations(callback);
});

gulp.task('readLocationMappingIntoRedis', function (callback) {
  systemdata.readLocationMappingIntoRedis(callback);
});

gulp.task('convertReasonTypes', ['readReasonTypeMappingIntoRedis'], function (callback) {
  systemdata.convertReasonTypes(callback);
});

gulp.task('readReasonTypeMappingIntoRedis', function (callback) {
  systemdata.readReasonTypeMappingIntoRedis(callback);
});

gulp.task('convertActionTypes', ['readActionTypeMappingIntoRedis'], function (callback) {
  systemdata.convertActionTypes(callback);
});

gulp.task('readActionTypeMappingIntoRedis', function (callback) {
  systemdata.readActionTypeMappingIntoRedis(callback);
});

gulp.task('convertInterests', ['readInterestMappingIntoRedis'], function (callback) {
  systemdata.convertInterests(callback);
});

gulp.task('readInterestMappingIntoRedis', function (callback) {
  systemdata.readInterestMappingIntoRedis(callback);
});

gulp.task('convertInterestParents', function (callback) {
  systemdata.convertInterestParents(callback);
});

gulp.task('convertPermissions', ['readPermissionMappingIntoRedis'], function (callback) {
  systemdata.convertPermissions(callback);
});

gulp.task('readPermissionMappingIntoRedis', function (callback) {
  systemdata.readPermissionMappingIntoRedis(callback);
});






gulp.task('publishers', ['convertPublishers', 'convertSubscriptions']);

gulp.task('convertPublishers', ['readPublisherMappingIntoRedis'], function (callback) {
  publishers.convertPublishers(callback);
});

gulp.task('readPublisherMappingIntoRedis', function (callback) {
  publishers.readPublisherMappingIntoRedis(callback);
});

gulp.task('convertSubscriptions', ['readSubscriptionMappingIntoRedis'], function (callback) {
  publishers.convertSubscriptions(callback);
});

gulp.task('readSubscriptionMappingIntoRedis', function (callback) {
  publishers.readSubscriptionMappingIntoRedis(callback);
});




gulp.task('members', ['']);

gulp.task('convertMembers', function (callback) {
  members.convertMembers(callback);
});

// Bruger import-list
gulp.task('readUserIdIntoRedis', function (callback) {
  members.readUserIdIntoRedis(callback);
});

gulp.task('readBrugereIntoRedis', function (callback) {
  members.readBrugereIntoRedis(callback);
});

// To be used for importing signups (subscription_member and permission_member), signouts and user_actions.
gulp.task('readMembersUserIdMappingIntoRedis', function (callback) {
  members.readMembersUserIdMappingIntoRedis(callback);
});


gulp.task('convertUserActions', function (callback) {
  members.convertUserActions(callback);
});

gulp.task('readUserActionsIntoRedis', function (callback) {
  members.readUserActionsIntoRedis(callback);
});

gulp.task('convertInteresseLinier', function (callback) {
  members.convertInteresseLinier(callback);
});

gulp.task('readInteresseLinierIntoRedis', function (callback) {
  members.readInteresseLinierIntoRedis(callback);
});


gulp.task('memberships', ['']);

gulp.task('convertSignups', ['readMembersUserIdMappingIntoRedis', 'readSignupsIntoRedis'], function (callback) {
  memberships.convertSignups(callback);
});

gulp.task('readSignupsIntoRedis', function (callback) {
  memberships.readSignupsIntoRedis(callback);
});

gulp.task('convertSignouts', ['readMembersUserIdMappingIntoRedis', 'readSignoutsIntoRedis'], function (callback) {
  memberships.convertSignouts(callback);
});

gulp.task('readSignoutsIntoRedis', function (callback) {
  memberships.readSignoutsIntoRedis(callback);
});

gulp.task('convertOptOuts', function (callback) {
  memberships.convertOptOuts(callback);
});

gulp.task('readOptOutsIntoRedis', function (callback) {
  memberships.readOptOutsIntoRedis(callback);
});


