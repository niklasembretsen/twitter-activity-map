# twitter-activity-map
The stream of tweets posted by users in real-time is too huge to process by regular computing means. This project will investigate how to use Apache Spark and Apache Cassandra to track regional real-time twitter activity.

## Tools
We will use the *Twitter streaming API* and *Apache Bahir Streaming Twitter* in order to create a direct stream to Twitter. Then some data-manipulation in *Apache Spark*. The result from that data-manipulation will be saved to *Apache Cassandra*. Then in order to show the results we will set up a *Node.js* server that will both fetch data from Cassandra and serve a JavaScript file with code for *D3* to visualize said data. This flow is presented below.

![Alt text](flowchartintensiveproject.png?raw=true "Title")

## Data
This project will use Twitter data collected from the [Twitter streaming API](https://developer.twitter.com/en/docs/tweets/filter-realtime/overview).

## Methodology and algorithm
To extract the information needed from the Twitter stream we propose the following approach:
- Map tweets to regions
- Reduce, or map with state, tweets to number of tweets per region
- Visualize this data with a world-heat-map

In addition, the following could also be implemented if we have time:
- Most popular words per region
- Real-time tweets, show dots on the heat map where a tweet is sent
