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
			SELECT activity, latitude, longitude
			FROM regions
			WHERE activity > 0
			ALLOW FILTERING`
		);
	}
}

module.exports = CassandraModel;