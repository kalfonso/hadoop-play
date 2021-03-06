package com.shinto.twitter;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterSpout extends BaseRichSpout {
    private Map<String, String> twitterConfig;
    private LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
    private TwitterStream twitterStream;
    private SpoutOutputCollector spoutOutputCollector;

    public TwitterSpout(Map<String, String> twitterConfig) {
        this.twitterConfig = twitterConfig;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        StatusListener statusListener = new TwitterStatusListener(queue);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        configurationBuilder.setDebugEnabled(true)
            .setOAuthConsumerKey(twitterConfig.get("consumer.key"))
            .setOAuthConsumerSecret(twitterConfig.get("consumer.secret"))
            .setOAuthAccessToken(twitterConfig.get("access.token"))
            .setOAuthAccessTokenSecret(twitterConfig.get("access.token.secret"));

        twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        twitterStream.addListener(statusListener);
        twitterStream.sample();
    }

    public void nextTuple() {
        Status status = queue.poll();
        if (status != null) {
            spoutOutputCollector.emit(new Values(status));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
        super.close();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config stormConfig = new Config();
        stormConfig.setMaxTaskParallelism(1);
        return stormConfig;
    }
}
