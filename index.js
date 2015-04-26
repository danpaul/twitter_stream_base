var Twitter = require('twitter');

var SCHEMA = function(table){
    table.increments()
    table.string('tracking').index()
    table.integer('created').index()
    table.json('tweet')
}

var DEFAULT_TABLE_NAME = 'tweets';

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

        statement
            .then(function(rows){ callback(null, rows) })
            .catch(callback)
    }

    self.track = function(toTrack){
        self.twitterClient.stream('statuses/filter',
                                  {track: toTrack}, function(stream) {


            stream.on('data', new self.newTweet(toTrack));
            stream.on('error', self.streamError);
        });
    }

    self.newTweet = function(trackingIn){
        var tracking = trackingIn;

        return function(tweet){
            var timeStamp = Date.now() / 1000;
            self.knex(self.tableName)
                .insert({
                    'tracking': tracking,
                    'tweet': JSON.stringify(tweet),
                    created: timeStamp
                })
                .then(function(){})
                .error(function(err){ console.log(err); })
        }
    }

    self.streamError = function(error){
        console.log(error)
    }

    self.init();

}