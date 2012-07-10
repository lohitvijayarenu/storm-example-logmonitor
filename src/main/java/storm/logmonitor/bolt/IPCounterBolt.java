package storm.logmonitor.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class IPCounterBolt extends BaseBasicBolt {
  private Map<String, Integer> map = new HashMap<String, Integer>();

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("state", "count"));
  }

 
  public void execute(Tuple input, BasicOutputCollector collector) {
    String state = input.getString(0);
    Integer count = input.getInteger(1);
    Integer requestCount = map.get(state);
    if (requestCount == null) 
      requestCount = 0;
    requestCount += count;
    map.put(state, requestCount);
    collector.emit(new Values(state, requestCount));
  }
}
