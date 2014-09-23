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



// Needs:
// - mapLocationsIntoRedis
gulp.task('convertLocations', function (callback) {
  systemdata.convertLocations(callback);
});

gulp.task('mapLocationsIntoRedis', function (callback) {
  systemdata.mapLocationsIntoRedis(callback);
});

// Needs:
// - mapReasonTypeIntoRedis
gulp.task('convertReasonTypes', function (callback) {
  systemdata.convertReasonTypes(callback);
});

gulp.task('mapReasonTypeIntoRedis', function (callback) {
  systemdata.mapReasonTypeIntoRedis(callback);
});


// Needs:
// - mapActionTypeIntoRedis
gulp.task('convertActionTypes', function (callback) {
  systemdata.convertActionTypes(callback);
});

gulp.task('mapActionTypeIntoRedis', function (callback) {
  systemdata.mapActionTypeIntoRedis(callback);
});


// Needs:
// - mapInterestIntoRedis
gulp.task('convertInterests', function (callback) {
  systemdata.convertInterests(callback);
});

gulp.task('mapInterestIntoRedis', function (callback) {
  systemdata.mapInterestIntoRedis(callback);
});

// Needs:
// - mapPermissionIntoRedis
gulp.task('convertInterestParents', function (callback) {
  systemdata.convertInterestParents(callback);
});

gulp.task('convertPermissions', function (callback) {
  systemdata.convertPermissions(callback);
});

gulp.task('mapPermissionIntoRedis', function (callback) {
  systemdata.mapPermissionIntoRedis(callback);
});






// Needs:
// - mapPublisherIntoRedis
gulp.task('convertPublishers', function (callback) {
  publishers.convertPublishers(callback);
});

gulp.task('mapPublisherIntoRedis', function (callback) {
  publishers.mapPublisherIntoRedis(callback);
});

// Needs:
// - mapSubscriptionIntoRedis
gulp.task('convertSubscriptions', function (callback) {
  publishers.convertSubscriptions(callback);
});

gulp.task('mapSubscriptionIntoRedis', function (callback) {
  publishers.mapSubscriptionIntoRedis(callback);
});



// Needs:
// - readBrugereIntoRedis
// - mapMembersIntoRedis
gulp.task('convertMembers', function (callback) {
  members.convertMembers(callback);
});

gulp.task('readBrugereIntoRedis', function (callback) {
  members.readBrugereIntoRedis(callback);
});

// Also, To be used for importing signups (subscription_member and permission_member), signouts and user_actions.
gulp.task('mapMembersIntoRedis', function (callback) {
  members.mapMembersIntoRedis(callback);
});


gulp.task('convertUserActions', function (callback) {
  members.convertUserActions(callback);
});

gulp.task('readUserActionsIntoRedis', function (callback) {
  members.readUserActionsIntoRedis(callback);
});


// Needs:
// - mapMembersIntoRedis
// - readInteresseLinierIntoRedis
// - mapLocationsIntoRedis
gulp.task('convertInteresseLinier', function (callback) {
  members.convertInteresseLinier(callback);
});

gulp.task('readInteresseLinierIntoRedis', function (callback) {
  members.readInteresseLinierIntoRedis(callback);
});




// Needs:
// - mapMembersIntoRedis
// - readSignupsIntoRedis
gulp.task('convertSignups', function (callback) {
  memberships.convertSignups(callback);
});

gulp.task('readSignupsIntoRedis', function (callback) {
  memberships.readSignupsIntoRedis(callback);
});



// Needs:
// - mapMembersIntoRedis
// - readSignoutsIntoRedis
gulp.task('convertSignouts', function (callback) {
  memberships.convertSignouts(callback);
});

gulp.task('readSignoutsIntoRedis', function (callback) {
  memberships.readSignoutsIntoRedis(callback);
});



// Needs:
// - mapMembersIntoRedis
// - readOptOutsIntoRedis
gulp.task('convertOptOuts', function (callback) {
  memberships.convertOptOuts(callback);
});

gulp.task('readOptOutsIntoRedis', function (callback) {
  memberships.readOptOutsIntoRedis(callback);
});


