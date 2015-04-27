var async = require('async');
var Twitter = require('twitter');

var SCHEMA = function(table){
    table.increments()
    table.string('tracking').index().default("")
    table.integer('created').index()
    table.boolean('parsed').index().default(false)
    table.text('trackingArray').default("")
    table.json('tweet')
}

var DEFAULT_TABLE_NAME = 'tweets';
var DEFAULT_PROCESS_BATCH_SIZE = 1000;
var PARALLEL_LIMIT = 10;

// twitter config follows: https://www.npmjs.com/package/twitter
// knexObject is an initialized knex object
module.exports = function(twitterConfig, knexObject, options){
    var self = this;

    self.init = function(){
        var options = options ? options : {};
        self.twitterClient = new Twitter(twitterConfig);
        self.knex = knexObject;
        self.tableName = options.tableName ?
            options.tableName : DEFAULT_TABLE_NAME;
        self.proccessBatchSize = options.proccessBatchSize ?
            options.proccessBatchSize : DEFAULT_PROCESS_BATCH_SIZE;
        self.initDB();
    }

    self.initDB = function(){

        // check if table exists
        self.knex.schema.hasTable(self.tableName)
            .then(function(exists) {
                if( !exists ){
                    // create the table
                    self.knex.schema.createTable(self.tableName, SCHEMA)
                        .then(function(){})
                        .catch(function(err){ throw(err); })

                }
            })
            .catch(function(err){ throw(err); })
    }

    // start and endIn are unix timestamps or null
    // start must be set if endIn is used
    self.get = function(tracking, start, end, callback){

        var statement = self.knex(self.tableName)
            .where('tracking', '=', tracking)

        if( start !== null ){
            statement.andWhere('created', '>', start)
            if( end !== null ){
                statement.andWhere('created', '<', end)
            }
        }

        statement.andWhere('parsed', '=', true)

        statement
            .then(function(rows){ callback(null, rows) })
            .catch(callback)
    }

    // toTrack is an array strings to track
    self.track = function(toTrack){

        var trackingString = '';

        toTrack.forEach(function(trackedItem){
            trackingString += trackedItem + ',';
        })

        self.twitterClient.stream('statuses/filter',
                                  {track: trackingString}, function(stream) {

            stream.on('data', new self.newTweet(toTrack));
            stream.on('error', self.streamError);
        });
    }

    self.newTweet = function(trackingArray){
        return function(tweet){
            var timeStamp = Date.now() / 1000;
            self.knex(self.tableName)
                .insert({
                    // 'tracking': JSON.stringify(trackingArray),
                    trackingArray: JSON.stringify(trackingArray),
                    'tweet': JSON.stringify(tweet),
                    created: timeStamp
                })
                .then(function(){})
                .error(function(err){ console.log(err); })
        }
    }

    self.processTweets = function(callbackIn){
        var reachedEnd = false;

        async.whilst(
            function(){ return !reachedEnd; },
            function(callback){
                self.knex(self.tableName)
                    .where('parsed', '=', false)
                    .orderBy('created', 'asc')
                    .limit(self.proccessBatchSize)
                    .then(function(rows){
                        if( rows.length === 0 ){
                            reachedEnd = true;
                            callback();
                        } else {
                            self.processTweetGroup(rows, callback)
                        }
                    })
                    .catch(callback)
            },
            callbackIn)
    }

    self.processTweetGroup = function(tweets, callbackIn){
        async.eachLimit(tweets, PARALLEL_LIMIT, function(tweetData, callback){
            var trackArray = JSON.parse(tweetData.trackingArray);

            async.eachSeries(trackArray, function(track, callbackB){
                if( tweetData.tweet.toLowerCase().indexOf(track.toLowerCase())
                        !== -1 ){

                    self.saveTweet(track, tweetData, callbackB);
                } else {
                    callbackB();
                }
            }, function(err){
                if( err ){
                    callback(err);
                    return;
                }
                // delete row after processing
                self.knex(self.tableName)
                    .where('id', '=', tweetData.id)
                    .delete()
                    .then(function(){ callback(); })
                    .catch(callback)
            })
        }, callbackIn)
    }

    self.saveTweet = function(tracking, tweetData, callbackIn){
        self.knex(self.tableName)
            .insert({
                'tracking': tracking,
                'tweet': tweetData.tweet,
                'trackingArray': '',
                created: tweetData.created,
                parsed: true
            })
            .then(function(){ callbackIn(); })
            .error(callbackIn)
    }

    self.streamError = function(error){
        console.log(error)
    }

    self.init();

}