package com.shinto.twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

import static com.shinto.twitter.TwitterHashtagFunction.getHashtag;

/**
 * Created by karel_alfonso on 16/10/2016.
 */
public class TwitterTermFrequencyTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("twitter-updates", new TwitterSpout())
            .each(new Fields("tweet"), getHashtag(), new Fields("hashtag"))
            .groupBy(new Fields("hashtag"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .newValuesStream()
            .each(new Fields("hashtag", "count"), new Debug())
            .parallelismHint(3);
        
        StormTopology stormTopology = topology.build();

        final Config conf = new Config();
        if(args.length==0 || args[0].equalsIgnoreCase("local")) {
            final LocalCluster local = new LocalCluster();
            local.submitTopology("twitter-topology", conf, stormTopology);
        } else if (args[0].equalsIgnoreCase("remote")) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology("twitter-topology", conf, stormTopology);
        } else
            System.out.println("Wrong argument provided: no args, local or remote");
    }
}
