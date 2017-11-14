package com.flipkart.rabbitmqspoutexample;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import org.apache.commons.collections4.map.LinkedMap;

import java.util.*;

public class SimpleSpout extends BaseRichSpout{

    private LinkedMap<Long, Long > ackTimestampsMap;

    private Double targetedQps = 100.0;

    private Double idealTimeGapForEmitting = (1.0*1000)/ targetedQps;

    private Double usedTimeGapForEmitting = idealTimeGapForEmitting;

    private Double MIN_TIME_GAP_FOR_EMITTING = 1.0;

    private int LAST_K_FOR_QPS = 10;

    private int WINDOW_SIZE = 100;

    private int SMOOTHING_FACTOR = WINDOW_SIZE/2;

    private Long lastEmittedTupleTS;

    SpoutOutputCollector outputCollector;
    ContentMetadata contentMetadata = new ContentMetadata("STORES", "s1", "pc1");

    public SimpleSpout(){
        this.ackTimestampsMap = new LinkedMap<>();
        this.lastEmittedTupleTS = Time.currentTimeMillis();
    }

    public void open(Map var1, TopologyContext var2, SpoutOutputCollector outputCollector){
        this.outputCollector = outputCollector;
    }

    @Override
    public void ack(Object msgId){
        Long endTime = Time.currentTimeMillis();
        Long startTime = (Long)msgId;
        while(ackTimestampsMap.size() > WINDOW_SIZE){
            ackTimestampsMap.remove(0);
        }
        ackTimestampsMap.put(startTime,endTime);
        updateTimeGap();
        System.out.println("Current QPS achieved:" + getQPS(ackTimestampsMap, 10));
    }

    @Override
    public void fail(Object msgId){

    }

    public void nextTuple(){
        Long currentTimeMillis = Time.currentTimeMillis();
        if(currentTimeMillis - lastEmittedTupleTS > usedTimeGapForEmitting){
            outputCollector.emit(new Values(contentMetadata), currentTimeMillis);
            lastEmittedTupleTS = currentTimeMillis;
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("contentMetadata"));
    }

    private double getQPS(LinkedMap<Long, Long> timestampsMap, int windowSize) {
        if(timestampsMap.size() == 0)
            return 0;
        if(timestampsMap.size() == 1)
            return 1;
        int size = timestampsMap.size();
        Long startTimestamp = (size > windowSize ? timestampsMap.get(size  - windowSize - 1) : timestampsMap.firstKey());
        Long timeWindow = timestampsMap.get(timestampsMap.lastKey()) - startTimestamp ;
        return (timestampsMap.size()*1000.0)/(1.0*timeWindow);
    }

    private void updateTimeGap(){
        if(ackTimestampsMap.size() <= 1) {
            return;
        }
        Double achievedQps = getQPS(ackTimestampsMap, WINDOW_SIZE);
        Double qpsDiff = targetedQps - achievedQps;
        Double timeGapDelta = -1000.0/(qpsDiff * SMOOTHING_FACTOR * 1.0);
        Double previousTimeGapForEmitting = usedTimeGapForEmitting;
        //TimeGapForEmitting should always be zero, avoid update if otherwise
        if(usedTimeGapForEmitting + timeGapDelta > 0) {
            usedTimeGapForEmitting = usedTimeGapForEmitting + timeGapDelta;
        }else{
            usedTimeGapForEmitting = MIN_TIME_GAP_FOR_EMITTING;
        }
        System.out.println("Previous timeGapForEmitting:" + previousTimeGapForEmitting + " Updated timeGapForEmitting:" + usedTimeGapForEmitting);
    }

}
