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
		// How many coordinates should a bounding box cover?
		val interval = 5

		// Make sure all bb-ids are present in the batches
		val numOfBoxes = (360/interval)*(180/interval)
		var allBoxes = List[(Int, (Int, Double, Double))]()
		for (i <- 1 to numOfBoxes) {
			val centerCoordinates = getCoordinatesFromBBID(i, interval)
			allBoxes = allBoxes :+ (i , (0, centerCoordinates._1, centerCoordinates._2))
		}

		// connect to Cassandra and make a keyspace and table as explained in the document
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        val session = cluster.connect()

        session.execute("""
        	DROP KEYSPACE IF EXISTS twitter_keyspace;""")

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS
            twitter_keyspace
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")

        session.execute("""
            CREATE TABLE IF NOT EXISTS
            twitter_keyspace.regions (bbid int PRIMARY KEY, latitude double, longitude double, activity bigint);""")

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

		// convert the list to a rdd
		var allBoxesRDD = ssc.sparkContext.parallelize(allBoxes)

		// Strart Receive
		// Whole tweet
		val stream = TwitterUtils.createFilteredStream(ssc, None, Some(locationsQuery))

		val batch = stream.map(tweet => {
			var coordinates = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
			val location = coordinates.getOrElse("(no location)")
			location
		})
		// Remove tweets with no location
		.filter(x => {
			x != "(no location)"
		})
		// Remove all info except coordinates
		.map(location => {
			(location.split(",")(0).toDouble,location.split(",")(1).toDouble)
		})
		// Convert coordinates to a grid id
		.map(coordinates => {
			val idAndCoords = getBBIdFromCoord(coordinates._1, coordinates._2, interval)
			(idAndCoords._1, (1, idAndCoords._2, idAndCoords._3))
		})

		var fullMapActivity = batch.transform(x => x.fullOuterJoin(allBoxesRDD))

		var fullMapActivityFiltered = fullMapActivity.map(x => {
			x match {
				case (bbid: Int, (None, value: Some[(Int, Double, Double)])) => (bbid, value.get)
				case (bbid: Int, (valTwitt: Some[(Int, Double, Double)], _)) => (bbid, valTwitt.get)
			}
		})

		// Sliding window of activity
        def mappingFunc(key: Int, value: Option[(Int, Double, Double)], state: State[List[(Long, Int)]]): (Int, Double, Double, Int) = {
            var currentSum = 0;
            if (state.exists) {
            	val newActivity = value.get._1
            	var allActivity = state.get

            	// Remove all entrys older than 1 minute
            	var continueToRemove = true
            	while (continueToRemove && allActivity.length > 0) {
	            	if (allActivity(0)._1 < (System.currentTimeMillis - 60000)) {
	            		val popped :: newAllActivity = allActivity
	            		allActivity = newAllActivity
	            	}
	            	else {
	            		continueToRemove = false;
	            	}
            	}

            	allActivity = allActivity :+ (System.currentTimeMillis, newActivity)
            	currentSum = allActivity.foldLeft(0)((acc, curr) => acc + curr._2)

                state.update(allActivity)    // Set the new state
            } else {
            	currentSum = value.get._1
            	val initialState = List((System.currentTimeMillis, currentSum))
                state.update(initialState)  // Set the initial state
            }
            // Return value
            (key, value.get._2, value.get._3, currentSum)
        }
        val stateDstream = fullMapActivityFiltered.mapWithState[List[(Long, Int)], (Int, Double, Double, Int)](StateSpec.function[Int,(Int, Double, Double),List[(Long, Int)],(Int, Double, Double, Int)](mappingFunc _))

		//store the result in Cassandra
        stateDstream.saveToCassandra("twitter_keyspace", "regions", SomeColumns("bbid", "latitude", "longitude", "activity"))

		ssc.checkpoint("file:/tmp/")
		ssc.start()
		ssc.awaitTermination()
		session.close()
	}

	/**
	 * Function that calculates IDs of boxes we use to split up the world map.
	 * I.e. split the coordinate ranges into evenly sized boxes, where the
	 * first box is indexed as zero and indexes raises row-vise.
	 * Example(
	 *    If interval is 90, we will get 8 boxes, indexed from 0 to 7
	 * )
	 */
	def getBBIdFromCoord(lat: Double, long: Double, interval: Int): (Int, Double, Double) = {

		// Get number of cols/rows in the grid
		val numCol = 360/interval
		val numRows = 180/interval

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

		// ¯\_(ツ)_/¯ (same as col, but different)
		if (rowFactor < 0) {
			rowIndex = Math.ceil(((numRows-1)/2D) - Math.ceil(rowFactor)).toInt
		}
		else {
			rowIndex = Math.floor(((numRows-1)/2D) - Math.floor(rowFactor)).toInt
		}

		// Handle cases where the coodinates is -180, 180, -360 or 360
		if (colIndex < 0) {
			colIndex = 0
		}
		else if (colIndex > numCol) {
			colIndex = (numCol-1).toInt
		}

		if (rowIndex < 0) {
			rowIndex = 0
		}
		else if(rowIndex > numRows) {
			rowIndex = (numRows-1).toInt
		}

		// Get the coordinates of the center for the specific box
		val centerLat = (((numRows/2D) - rowIndex) * interval) - (interval/2D)
		val centerLong = ((colIndex - (numCol/2D)) * interval) + (interval/2D)

		// Use number of boxes per row times the number of rows and then add number of boxes on current row
		val bbID = (rowIndex * numCol) + colIndex

		if (bbID < 0) {
			println("lat: " + lat + " long: " + long + " rowIndex: " + rowIndex + " colIndex: " + colIndex)
			System.exit(0)
		}

		(bbID.toInt, centerLat, centerLong)
	}

	/**
	 * function that returns the center coordinates for a single box with given bbid
	 */
	def getCoordinatesFromBBID(bbid: Int, interval: Int): (Double, Double) = {
		// Get number of cols/rows in the grid
		val numCol = 360/interval
		val numRows = 180/interval

		var rowIndex = Math.floor(bbid/numRows)
		var colIndex = bbid % numCol

		// Get the coordinates of the center for the specific box
		val centerLat = (((numRows/2D) - rowIndex) * interval) - (interval/2D)
		val centerLong = ((colIndex - (numCol/2D)) * interval) + (interval/2D)

		(centerLat, centerLong)
	}
}
