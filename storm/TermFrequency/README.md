How to run

HDFS_URL=hdfs://namenode:8020 TWITTERSTREAM_HDFS_PATH=/user/ec2-user/twitterstream storm jar TermFrequency-1.0-SNAPSHOT.jar com.shinto.twitter.TwitterTermFrequencyTopology -c nimbus.host=node3