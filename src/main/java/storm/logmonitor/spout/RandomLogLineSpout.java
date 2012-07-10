package storm.logmonitor.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomLogLineSpout extends BaseRichSpout {

  SpoutOutputCollector _collector;
  Random _rand;

  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random(1);
  }

  /**
   * Emit random log like lines
   */
  public void nextTuple() {
    // Sleep a while before emitting next tuple
    Utils.sleep(1000);
    String randomLogLine = generateRandomIP() + String.valueOf(generateRandomState()) + 
        " click log blah blah ";
    _collector.emit(new Values(randomLogLine)); 
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ip"));
  }
  
  private String generateRandomIP() {
    return String.valueOf(_rand.nextInt(256)) + String.valueOf(_rand.nextInt(256)) +
        String.valueOf(_rand.nextInt(256)) + String.valueOf(_rand.nextInt(256));
  }
  
  private int generateRandomState() {
    return _rand.nextInt() % 50;
  }

}
