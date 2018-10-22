
/**
 * Dependecies
 */
var async = require('async');
var cassandra = require('cassandra-driver');
var CassandraModel = require('./cassandra.js');
var express = require('express');
var path = require('path');
var dateTime = require('node-datetime');

// Create the express instance
const server = express();

// Connect cassandra cluster
const cassandraClient = new cassandra.Client({
	contactPoints: ['127.0.0.1'],
	keyspace: 'twitter_keyspace'
});

// Instance to our db model
const db =  new CassandraModel(cassandraClient);


// Serve a static folder "public" on /static
server.use('/static', express.static('public'));

// Listen to /(startpage)
server.get('/', (req, res) => {
	// Serve static html file
	res.sendFile(path.join(__dirname, 'index.html'));
});

// Listen to /data
server.get('/data', (req, res) => {
	var now = dateTime.create();
	var oneMinuteAgo = dateTime.create(now.getTime() - 60000*121);
	var timeFormatted = oneMinuteAgo.format('Y-m-d H:M:S');
	console.log(timeFormatted)
	// First fetch data
	db.fetchRegionalData(timeFormatted).then(results => {
		// Send back json-result
		res.json(results.rows);
	});
});


server.listen(3000);
console.log('listening');

// Kill node
process.on('SIGINT', function() {
	process.exit();
})