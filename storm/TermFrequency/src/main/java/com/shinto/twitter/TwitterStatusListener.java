package com.shinto.twitter;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by karel_alfonso on 16/10/2016.
 */
public class TwitterStatusListener implements StatusListener {
    private final LinkedBlockingQueue<Status> queue;

    public TwitterStatusListener(LinkedBlockingQueue<Status> queue) {
        this.queue = queue;
    }

    public void onStatus(Status status) {
        queue.offer(status);
    }

    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    public void onTrackLimitationNotice(int i) {

    }

    public void onScrubGeo(long l, long l1) {

    }

    public void onStallWarning(StallWarning stallWarning) {

    }

    public void onException(Exception e) {

    }
}
