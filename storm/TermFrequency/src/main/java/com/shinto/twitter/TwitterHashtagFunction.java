package com.shinto.twitter;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;


public class TwitterHashtagFunction extends BaseFunction {
    public static Function getHashtag() {
        return new TwitterHashtagFunction();
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        for (HashtagEntity hashtagEntity: tweet.getHashtagEntities()) {
            collector.emit(new Values(hashtagEntity.getText()));
        }
    }


}
