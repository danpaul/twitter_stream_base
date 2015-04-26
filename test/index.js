var twitterConfig = require('../twitter_config');
var TwitterStreamBase = require('../index');

var dbCreds = {
    client: 'mysql',
    connection: {
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'twitter_stream_base',
        port:  8889
    }
}

var knex = require('knex')(dbCreds)

var twitterStreamBase = new TwitterStreamBase(twitterConfig, knex);

twitterStreamBase.track('nyc');