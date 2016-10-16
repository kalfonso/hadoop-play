package com.shinto;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;


public class WordCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
            new Values("the cow jumped over the moon"),
            new Values("the man went to the store and bought some candy"),
            new Values("four score and seven years ago"),
            new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
            .each(new Fields("sentence"), new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
            .newValuesStream()
            .each(new Fields("word", "count"), new Debug())
            .parallelismHint(6);
        StormTopology stormTopology = topology.build();

        final Config conf = new Config();
        if(args.length==0) {
            final LocalCluster local = new LocalCluster();
            local.submitTopology("word-count-topology", conf, stormTopology);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology("word-count-topology", conf, stormTopology);
        }
    }
}
