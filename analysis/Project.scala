package sparkstreaming

import scala.io.Source
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
import twitter4j.FilterQuery

object Project {
	def main(args: Array[String]) {
		// connect to Cassandra and make a keyspace and table as explained in the document
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        val session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS
            twitter_keyspace
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")

        session.execute("""
        	DROP TABLE IF EXISTS twitter_keyspace.regions""")

        session.execute("""
            CREATE TABLE IF NOT EXISTS
            twitter_keyspace.regions (time timestamp PRIMARY KEY, bbid int, activity bigint);""")

		// Remove info prints
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)


		// Read Twitter credentials
		val source: String = Source.fromFile("cred.txt").getLines.mkString
		val creds = source.split(",")

		// Set Twitter credentials
		System.setProperty("twitter4j.oauth.consumerKey", creds(0))
		System.setProperty("twitter4j.oauth.consumerSecret", creds(1))
		System.setProperty("twitter4j.oauth.accessToken", creds(2))
		System.setProperty("twitter4j.oauth.accessTokenSecret", creds(3))

		// Config for the stream
		val sparkConf = new SparkConf().setAppName("Twitter-heat-map")

		// check Spark configuration for master URL, set it to local if not configured
		if (!sparkConf.contains("spark.master")) {
			sparkConf.setMaster("local[*]")
		}

		// Set out bounding box to the whole world
		val southWest = Array(-180D, -90D)
		val northEast = Array(180, 90D)
		val boundingBoxes = Array(southWest, northEast)
		val locationsQuery = new FilterQuery().locations(boundingBoxes : _*)

		// Receive a batch every second
		val ssc = new StreamingContext(sparkConf, Seconds(3))

		// Strart Receive
		val interval = 90
		val stream = TwitterUtils.createFilteredStream(ssc, None, Some(locationsQuery))
		.map(tweet => {
			var coordinates = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
			val location = coordinates.getOrElse("(no location)")
			location
		})
		.filter(x => {
			x != "(no location)"
		})
		.map(location => {
			(location.split(",")(0).toDouble,location.split(",")(1).toDouble)
		})
		.map(
			coordinates => {
				(getBBIdFromCoord(coordinates._1, coordinates._2, interval), 1)
		})
		.reduceByKey((acc, curr) => acc + curr)
		// Add timestamp
		.map(batch => (new Date(), batch._1, batch._2))
		// store the result in Cassandra
        .saveToCassandra("twitter_keyspace", "regions", SomeColumns("time","bbid", "activity"))

		ssc.start()
		ssc.awaitTermination()
	}

	/**
	 * Function that calculates IDs of boxes we use to split up the world map.
	 * I.e. split the coordinate ranges into evenly sized boxes, where the
	 * first box is indexed as zero and indexes raises row-vise.
	 * Example(
	 *    If interval is 90, we will get 8 boxes, indexed from 0 to 7
	 * )
	 */
	def getBBIdFromCoord(lat: Double, long: Double, interval: Int): Int = {

		// Get number of cols/rows in the grid
		val numCol = 360D/interval
		val numRows = 180D/interval

		//get number of steps from middle of grid
		val colFactor = long/interval
		val rowFactor = lat/interval

		var colIndex = 0
		var rowIndex = 0
		if (colFactor < 0) {
			// If we take negative steps from the middle, we need to ceil the colFactor, i.e. -1.6 -> -1
			colIndex = Math.floor(((numCol-1)/2D) + Math.ceil(colFactor)).toInt
		}
		else {
			// If we take positive steps from the middle, we need to floor the colFactor, i.e. 1.6 -> 1
			colIndex = Math.ceil(((numCol-1)/2D) + Math.floor(colFactor)).toInt
		}

		// wtf?! ¯\_(ツ)_/¯ (same as col, but different)
		if (rowFactor < 0) {
			rowIndex = Math.ceil(((numRows-1)/2D) - Math.ceil(rowFactor)).toInt
		}
		else {
			rowIndex = Math.floor(((numRows-1)/2D) - Math.floor(rowFactor)).toInt
		}

		// Use number of boxes per row times the number of rows and then add number of boxes on current row
		val bbID = (rowIndex * numCol) + colIndex
		bbID.toInt
	}
}
