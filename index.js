var _ = require('underscore');
var async = require('async');
var Twitter = require('twitter');
var r = require('rethinkdb');

var DEFAULT_DB_NAME = 'tweet_base';
var DEFAULT_TABLE_NAME = 'tweets';
var DEFAULT_PROCESS_BATCH_SIZE = 1000;
var PARALLEL_LIMIT = 10;
var WATCH_INTERVAL = 1000;

var ERROR_INIT = 'twitter_steram_base requires host and port'

// twitter config follows: https://www.npmjs.com/package/twitter
// options must include rethinkdb config: host, port
// options may include databaseName and tableName
// callback gets called after intialization
module.exports = function(twitterConfig, options, callbackIn){
    var self = this;
    self.tweetQue = [];

    self.init = function(){

        if( !options.host || !options.port ){ throw ERROR_INIT; }

        self.databaseName = options.databaseName ?
            options.databaseName : DEFAULT_DB_NAME;
        self.tableName = options.tableName ?
            options.tableName : DEFAULT_TABLE_NAME;

        self.twitterClient = new Twitter(twitterConfig);
        self.watchQue();

        async.series([
            function(callback){
                r.connect({host: options.host, port: options.port},
                          function(err, conn){

                    if( err ){ throw err; }
                    self.connection = conn;
                    callback();
                });
            },

            // check if db exists, create it if not
            function(callback){

                r.dbList().run(self.connection, function(err, dbs){
                    if( err ){
                        callback(err);
                        return;
                    }

                    if( _.contains(dbs, self.databaseName) ){
                        callback();
                        return;
                    }

                    r.dbCreate(self.databaseName).run(self.connection,
                                                      function(err){
                        if( err ){ callback(err); }
                        else{ callback(); }
                    });
                })
            },

            // check if table exists, create it if not
            function(callback){
                r.db(self.databaseName)
                    .tableList()
                    .run(self.connection, function(err, tables){
                        if( err ){
                            callback(err);
                            return;
                        }
                        if( _.contains(tables, self.tableName) ){
                            callback();
                            return;
                        }
                        r.db(self.databaseName)
                            .tableCreate(self.tableName)
                            .run(self.connection, function(err){
                                if( err ){
                                    callback(err);
                                    return;
                                }
                                callback();
                            });
                })
            }
        ], callbackIn)
    }

    // toTrack is an array strings to track
    self.track = function(toTrack){

        var trackingString = '';
        self.trackTerms = toTrack;

        toTrack.forEach(function(trackedItem){
            trackingString += trackedItem + ',';
        })

        self.twitterClient.stream('statuses/filter',
                                  {track: trackingString}, function(stream) {

            stream.on('data', new self.newTweet(toTrack));
            stream.on('error', self.streamError);
        });
    }

    self.watchQue = function(){

        setInterval(function(){
            if( self.tweetQue.length === 0 ){ return; }
// console.log(self.tweetQue.length)
            var tweets = self.tweetQue;
            self.tweetQue = [];
            tweets = self.cleanTweets(tweets);
            r.db(self.databaseName).table(self.tableName).insert(tweets)
                .run(self.connection, function(err){
                    if( err ){ console.log(err); }
                })


        }, WATCH_INTERVAL)
    }

    self.cleanTweets = function(tweets, searchTerms){
        var cleanTweets = [];
        _.each(tweets, function(tweet){
            if( tweet.id ){
                var master = {
                    twitter_id: tweet.id,
                    timestamp_ms: parseInt(tweet.timestamp_ms),
                    text: tweet.text,
                    user_id: tweet.user.id,
                    user_followers: tweet.user.followers_count
                }

                _.each(self.trackTerms, function(trackTerm){
                    if( master.text.toLowerCase()
                              .indexOf(trackTerm.toLowerCase()) !== -1 ){
                        var cleanTweet = _.clone(master);
                        cleanTweet.trackTerm = trackTerm;
                        cleanTweets.push(cleanTweet);
                    }
                })                
            }
        })
        return cleanTweets;
    }

    self.newTweet = function(trackingArray){
        return function(tweet){
            self.tweetQue.push(tweet);
        }
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