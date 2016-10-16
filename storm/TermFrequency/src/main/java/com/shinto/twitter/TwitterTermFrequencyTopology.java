package com.shinto.twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

/**
 * Created by karel_alfonso on 16/10/2016.
 */
public class TwitterTermFrequencyTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("twitter-updates", new TwitterSpout())
            .each(new Fields("tweet"), new Debug());


        StormTopology stormTopology = topology.build();

        final Config conf = new Config();
        if(args.length==0) {
            final LocalCluster local = new LocalCluster();
            local.submitTopology("twitter-topology", conf, stormTopology);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology("twitter-topology", conf, stormTopology);
        }
    }
}
