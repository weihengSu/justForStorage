
package com.microsoft.example;
import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


import com.microsoft.example.HashtagInput;
import com.microsoft.example.HashtagReader;

public class twitter {
   public static void main(String[] args) throws Exception{
      String cKey = args[0];
      String cSecret = args[1];
		
      String aToken = args[2];
      String aSecret = args[3];
		
      String[] args = args.clone();
      String[] key = Arrays.copyOfRange(args, 4, args.length);
		
      Config config = new Config();
      config.setDebug(true);
		
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("twitter-spout", new TwitterSampleSpout(cKey,
         cSecret, aToken, aSecret, key));

      builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
         .shuffleGrouping("twitter-spout");

      builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
         .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
			
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("TwitterHashtagStorm", config,builder.createTopology());
      Thread.sleep(4000);
      cluster.shutdown();
   }
}
