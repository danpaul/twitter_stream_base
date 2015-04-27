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

// twitterStreamBase.track(['nyc', 'doggies']);

twitterStreamBase.processTweets(function(err){
    if( err ){ throw(err); }
    else{ console.log('Success processing tweets.'); }
})

// twitterStreamBase.get('nyc', null, null, function(err, results){
//     if( err ){ throw(err); }
//     else{ console.log(results); }
// });

