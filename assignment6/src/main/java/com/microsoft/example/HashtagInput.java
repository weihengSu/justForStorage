
package com.microsoft.example;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class HashtagInput implements IRichBolt {
   Map<String, Integer> map;
   private OutputCollector collector;

   @Override
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.map = new HashMap<String, Integer>();
      this.collector = collector;
   }

   @Override
   public void execute(Tuple tuple) {
      String key = tuple.getString(0);

      if(!map.containsKey(key)){
         map.put(key, 1);
      }else{
         Integer c = counterMap.get(key) + 1;
         counterMap.put(key, c);
      }
		
      collector.ack(tuple);
   }

   @Override
   public void cleanup() {
      int i = 0;	 
      for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
         if (i<5){
          Date date= new Date();
         //getTime() returns current time in milliseconds
	 long time = date.getTime();
         //Passed the milliseconds to constructor of Timestamp class 
	 Timestamp ts = new Timestamp(time);
       System.out.println(ts + " "  + entry.getKey()+" : " + entry.getValue());
        i++
         }
      else{break;}
   }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
	
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
