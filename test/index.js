var twitterConfig = require('../twitter_config');
var TwitterStreamBase = require('../index');

var dbCred = {
    host: 'localhost', port: 28015
}

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

var tBase = new TwitterStreamBase(twitterConfig, dbCred, function(err){
    if( err ){ throw err; }

    // tBase.track(['love', 'peace', 'TheyreTheOne', 'win', 'one', 'nyc', 'la', 'index', 'javascript', 'dad', 'mom', 'god', 'dog', 'cat', 'food', 'hate', 'the']);

    // tBase.get('nyc', null, null, function(err, results){
    //     if( err ){ throw(err); }
    //     else{ console.log(results); }
    // });

    // tBase.get('nyc', 1430655114733, 1430655135633, function(err, results){
    //     if( err ){ throw(err); }
    //     else{ console.log(results); }
    // });

});

