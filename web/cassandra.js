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
	fetchRegionalData() {
		return this.client.execute(`
			SELECT bbid, activity, latitude, longitude
			FROM regions
			WHERE activity > 0
			ALLOW FILTERING`
		);
	}
}

module.exports = CassandraModel;