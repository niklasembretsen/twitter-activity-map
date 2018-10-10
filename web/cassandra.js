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
			SELECT *
			FROM regions`
		);
	}
}

module.exports = CassandraModel;