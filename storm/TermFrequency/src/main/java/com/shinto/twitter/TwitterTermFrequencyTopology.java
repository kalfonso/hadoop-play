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
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

import static com.shinto.twitter.TwitterHashtagFunction.getHashtag;


public class TwitterTermFrequencyTopology {

    private final StormTopology stormTopology;
    private final Config stormConfig;

    public TwitterTermFrequencyTopology() {
        TridentTopology topology = new TridentTopology();
        topology.newStream("twitter-updates", new TwitterSpout())
            .each(new Fields("tweet"), getHashtag(), new Fields("hashtag"))
            .groupBy(new Fields("hashtag"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .newValuesStream()
            .partitionPersist(hdfsStateFactory(), new Fields("hashtag", "count"), new HdfsUpdater(), new Fields())
                //.each(new Fields("hashtag", "count"), new Debug())
            .parallelismHint(3);

        stormTopology = topology.build();

        stormConfig = new Config();
        stormConfig.setDebug(true);
    }

    private StateFactory hdfsStateFactory() {
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPath(System.getenv("TWITTERSTREAM_HDFS_PATH"))
            .withPrefix("twitter-stream")
            .withExtension(".txt");

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(new DelimitedRecordFormat()
                .withFields(new Fields("hashtag", "count")))
            .withRotationPolicy(new FileSizeRotationPolicy(10.0f, FileSizeRotationPolicy.Units.MB))
            .withFsUrl(System.getenv("HDFS_URL"));

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
        TwitterTermFrequencyTopology topology = new TwitterTermFrequencyTopology();
        if(args.length==0 || args[0].equalsIgnoreCase("local")) {
            topology.submitLocal();
        } else if (args[0].equalsIgnoreCase("remote")) {
            topology.submitCluster();
        } else
            System.out.println("Wrong argument provided: no args, local or remote");
    }
}
