package com.flipkart.rabbitmqspoutexample;

import backtype.storm.tuple.MessageId;

import java.util.LinkedList;
import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Time;


public class SimpleBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Long dummyResponseTime = 50L;

    private LinkedList<Long> latencies;

    private int WINDOW_SIZE = 15;

    public SimpleBolt(){
        this.latencies = new LinkedList<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input){
        try{
            Long currentTimeInMillis = Time.currentTimeMillis();
            if(latencies.size() >= WINDOW_SIZE){
                latencies.remove();
            }
            latencies.add(currentTimeInMillis);
            System.out.println("Bolt execute timestamp:" + getQPS(latencies));
            ContentMetadata contentMetadata = (ContentMetadata)input.getValueByField("contentMetadata");
            MessageId messageId = input.getMessageId();
            Thread.sleep(dummyResponseTime);
            collector.ack(input);
        }catch(Exception e){
            System.err.println("Exception:" + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer var1){

    }

    private Double getQPS(LinkedList<Long> latencies){
        if(latencies.size() == 0){
            return 0.0;
        }
        if(latencies.size() == 1){
            return 1.0;
        }
        Long timeWindow = latencies.peekLast() - latencies.peekFirst();
        return (1000.0*(latencies.size()-1))/(1.0*timeWindow);
    }
}
