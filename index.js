var Twitter = require('twitter');

var SCHEMA = function(table){
    table.increments()
    table.string('tracking').index()
    table.text('tweet')
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

            self.knex(self.tableName)
                .insert({'tracking': tracking, 'tweet': JSON.stringify(tweet) })
                .then(function(){})
                .error(function(err){ console.log(err); })
        }
    }

    self.streamError = function(error){
        console.log(error)
    }

    self.init();

}