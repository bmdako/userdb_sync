var gulp = require('gulp'),
    jshint = require('gulp-jshint'),
    members = require('./src/members'),
    memberships = require('./src/memberships'),
    publishers = require('./src/publishers'),
    systemdata = require('./src/systemdata'),
    redis_helper = require('./src/redis_helper');

gulp.task('default', [
  'convertLocations',
  'convertPublishers',
  'convertSubscriptions' ], function () {
  // place code for your default task here
});

gulp.task('lint', function() {
  gulp.src('./src/**/*.js')
    .pipe(jshint())
    .pipe(jshint.reporter('default'));
});

// Needs:
// - cacheLocations
gulp.task('convertLocations', function (callback) {
  systemdata.convertLocations(callback);
});

gulp.task('cacheLocations', function (callback) {
  redis_helper.cacheLocations(callback);
});

// Needs:
// - cacheReasonType
gulp.task('convertReasonTypes', function (callback) {
  systemdata.convertReasonTypes(callback);
});

gulp.task('cacheReasonType', function (callback) {
  redis_helper.cacheReasonType(callback);
});


// Needs:
// - cacheActionType
gulp.task('convertActionTypes', function (callback) {
  systemdata.convertActionTypes(callback);
});

gulp.task('cacheActionType', function (callback) {
  redis_helper.cacheActionType(callback);
});


// Needs:
// - cacheInterest
gulp.task('convertInterests', function (callback) {
  systemdata.convertInterests(callback);
});

gulp.task('cacheInterest', function (callback) {
  redis_helper.cacheInterest(callback);
});

// Needs:
// - cachePermission
gulp.task('convertInterestParents', function (callback) {
  systemdata.convertInterestParents(callback);
});

gulp.task('convertPermissions', function (callback) {
  systemdata.convertPermissions(callback);
});

gulp.task('cachePermission', function (callback) {
  redis_helper.cachePermission(callback);
});





// Needs:
// - cachePublisher
gulp.task('convertPublishers', function (callback) {
  publishers.convertPublishers(callback);
});

gulp.task('cachePublisher', function (callback) {
  redis_helper.cachePublisher(callback);
});

// Needs:
// - cacheSubscription
gulp.task('convertSubscriptions', function (callback) {
  publishers.convertSubscriptions(callback);
});

gulp.task('cacheSubscription', function (callback) {
  redis_helper.cacheSubscription(callback);
});



// Needs:
// - copyBrugere
// - cacheMembers
gulp.task('convertMembers', function (callback) {
  members.convertMembers(callback);
});

gulp.task('copyBrugere', function (callback) {
  redis_helper.copyBrugere(callback);
});

// Also, To be used for importing signups (subscription_member and permission_member), signouts and user_actions.
gulp.task('cacheMembers', function (callback) {
  redis_helper.cacheMembers(callback);
});


gulp.task('convertUserActions', function (callback) {
  members.convertUserActions(callback);
});

gulp.task('copyUserActions', function (callback) {
  redis_helper.copyUserActions(callback);
});


// Needs:
// - cacheMembers
// - copyInteresseLinier
// - cacheInterestLines
// - cacheLocations
gulp.task('convertInteresseLinier', function (callback) {
  members.convertInteresseLinier(callback);
});

gulp.task('copyInteresseLinier', function (callback) {
  redis_helper.copyInteresseLinier(callback);
});

gulp.task('cacheInterestLines', function (callback) {
  redis_helper.cacheInterestLines(callback);
});




// Needs:
// - cacheMembers
// - cacheEmails
// - copySignups
// - cacheLocations
// - cachePermission
// - cacheSubscription
// - cacheSubscriptionMembers
// - cachePermissionMembers
gulp.task('convertSignups', function (callback) {
  memberships.convertSignups(callback);
});

gulp.task('cacheEmails', function (callback) {
  redis_helper.cacheEmails(callback);
});

gulp.task('cacheSubscriptionMembers', function (callback) {
  redis_helper.cacheSubscriptionMembers(callback);
});

gulp.task('cachePermissionMembers', function (callback) {
  redis_helper.cachePermissionMembers(callback);
});

gulp.task('copySignups', function (callback) {
  redis_helper.copySignups(callback);
});



// Needs:
// - cacheMembers
// - copySignouts
// - cacheMembers
// - cacheEmails
// - cachePermission
// - cacheSubscription
gulp.task('convertSignouts', function (callback) {
  memberships.convertSignouts(callback);
});

gulp.task('copySignouts', function (callback) {
  redis_helper.copySignouts(callback);
});



// Needs:
// - cacheMembers
// - copyOptOuts
// - cacheEmailIds
gulp.task('convertOptOuts', function (callback) {
  memberships.convertOptOuts(callback);
});

gulp.task('cacheEmailIds', function (callback) {
  redis_helper.cacheEmailIds(callback);
});

gulp.task('copyOptOuts', function (callback) {
  redis_helper.copyOptOuts(callback);
});


gulp.task('diffMembers', function (callback) {
  redis_helper.diffMembers(callback);
});

// gulp.task('missingInRedis', function (callback) {
//   members.missingInRedis(callback);
// });

// gulp.task('missingInMysql', function (callback) {
//   members.missingInMysql(callback);
// });

gulp.task('convertMissingMembers', function (callback) {
  members.convertMissingMembers(callback);
});
