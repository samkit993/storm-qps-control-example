package com.flipkart.rabbitmqspoutexample;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class SampleTopology {
    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("simpleSpout", new SimpleSpout());


        topologyBuilder.setBolt("simpleBolt", new SimpleBolt(), 20)
                .shuffleGrouping("simpleSpout");

        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SampleTopology", Utils.getStormConfiguration(), topology);
        System.out.println("Submitted storm topology to local cluster successfully.");
    }
}
