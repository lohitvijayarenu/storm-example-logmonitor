package storm.logmonitor.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterLogLineBolt extends BaseBasicBolt {
  private Map<String, Integer> countByState = new HashMap<String, Integer>();
  
  public void execute(Tuple input, BasicOutputCollector collector) {
    String line = input.getString(0);
    StringTokenizer tokenizer = new StringTokenizer(line);
    if (tokenizer.countTokens() > 2) {
      String ip = tokenizer.nextToken();
      // Do basic validation of ip to make sure we can count this as valid line
      String state = tokenizer.nextToken();
      Integer count = countByState.get(state);
      if (count == null)
        count = 0;
      count++;
      countByState.put(state, count);
      collector.emit("countbystate", new Values(state, count));
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ipbystate", "ipcount"));
  }
}
