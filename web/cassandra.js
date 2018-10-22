/**
 * This file holds the data-base calls
 */

class CassandraModel {

	constructor(client){
		this.client = client;
	}

	/**
	 * Function that fetches all regions from Cassandra
	 */
	fetchRegionalData(nowFormatted) {
		return this.client.execute(`
			SELECT bbid, activity
			FROM regions
			WHERE time > '`+ nowFormatted +`'
			ALLOW FILTERING`
		);
	}
}

module.exports = CassandraModel;