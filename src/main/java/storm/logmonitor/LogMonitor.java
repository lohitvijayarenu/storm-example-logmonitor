/**
 * Simple Storm Topology to monitor log files and emit interesting 
 * facts seen in logs
 */
package storm.logmonitor;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.logmonitor.spout.*;
import storm.logmonitor.bolt.*;

public class LogMonitor {
  public static void main(String[] args) 
      throws AlreadyAliveException, InvalidTopologyException {
    
    // Create topology with random log file generator spout
    // filter bolt and counter by ip address bolt
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new RandomLogLineSpout(), 5);
    builder.setBolt("filterlogline", new FilterLogLineBolt(), 8)
      .shuffleGrouping("spout");
    builder.setBolt("ipbystatecounter", new IPCounterBolt(), 12)
      .fieldsGrouping("filterlogline", new Fields("state"));
    
    Config conf = new Config();
    conf.setNumWorkers(3);

    // Submit topology with name log-monitor
    StormSubmitter.submitTopology("log-monitor", conf, builder.createTopology());
  }
}
