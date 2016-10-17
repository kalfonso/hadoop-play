package com.shinto.twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

import static com.shinto.twitter.TwitterHashtagFunction.getHashtag;


public class TwitterTermFrequencyTopology {

    private final StormTopology stormTopology;
    private final Config stormConfig;

    public TwitterTermFrequencyTopology(String hdfsUrl, String hdfsPath) {
        TridentTopology topology = new TridentTopology();
        topology.newStream("twitter-updates", twitterSpout())
            .each(new Fields("tweet"), getHashtag(), new Fields("hashtag"))
            .groupBy(new Fields("hashtag"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .newValuesStream()
            .partitionPersist(hdfsStateFactory(hdfsUrl, hdfsPath), new Fields("hashtag", "count"), new HdfsUpdater(), new Fields())
                //.each(new Fields("hashtag", "count"), new Debug())
            .parallelismHint(3);

        stormTopology = topology.build();

        stormConfig = new Config();
        stormConfig.setDebug(true);
    }

    private IRichSpout twitterSpout() {
        Map<String, String> twitterConfig = new HashMap<String, String>();
        twitterConfig.put("consumer.key", System.getenv("TWITTER_CONSUMER_KEY"));
        twitterConfig.put("consumer.secret", System.getenv("TWITTER_CONSUMER_SECRET"));
        twitterConfig.put("access.token", System.getenv("TWITTER_ACCESS_TOKEN"));
        twitterConfig.put("access.token.secret", System.getenv("TWITTER_ACCESS_TOKEN_SECRET"));

        return new TwitterSpout(twitterConfig);
    }

    private StateFactory hdfsStateFactory(String hdfsUrl, String hdfsPath) {
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPath(hdfsPath)
            .withPrefix("twitter-stream")
            .withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(new DelimitedRecordFormat()
                .withFields(new Fields("hashtag", "count")))
            .withRotationPolicy(new FileSizeRotationPolicy(10.0f, FileSizeRotationPolicy.Units.MB))
            .withFsUrl(hdfsUrl);

        return new HdfsStateFactory().withOptions(options);
    }

    private void submitCluster() {
        stormConfig.setNumWorkers(3);
        try {
            StormSubmitter.submitTopology("twitter-topology", stormConfig, stormTopology);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void submitLocal() {
        final LocalCluster local = new LocalCluster();
        local.submitTopology("twitter-topology", stormConfig, stormTopology);
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if (args.length < 2) {
            System.out.println("Expected arguments: hdfs_url, hdfs_path, execution_mode[local|remote]");
            System.exit(2);
        }
        TwitterTermFrequencyTopology topology = new TwitterTermFrequencyTopology(args[0], args[1]);
        submit(args, topology);
    }

    private static void submit(String[] args, TwitterTermFrequencyTopology topology) {
        if(args.length==2 || args[2].equalsIgnoreCase("local")) {
            topology.submitLocal();
        } else if (args[2].equalsIgnoreCase("remote")) {
            topology.submitCluster();
        } else
            System.out.println("Wrong argument provided: no args, local or remote");
    }
}
