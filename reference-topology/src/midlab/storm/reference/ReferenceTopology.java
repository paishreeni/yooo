package midlab.storm.reference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class ReferenceTopology {

	public static void main(String[] args) throws Exception {
		
		Config conf = new Config();
        conf.setDebug(true);
        conf.setStatsSampleRate(1.0);
        
        String topologyName = null;
        int randomCount = 1;
        int simpleCount = 1;
        int statefulCount = 1;
        int ackCount = 1;
        int workerCount = 1;
        int stageCount = 3;
        int changeTrafficStage = -1;
        int changeTrafficFactor = 1;
        if (args != null && args.length > 0) {
        	topologyName = args[0];
        	for (int i = 1; i < args.length; i++) {
        		String key = args[i++].substring(1);
        		String value = args[i];
        		conf.put(key, value);
        	}
        	if (conf.get("random.count") != null)
        		randomCount = Integer.parseInt((String)conf.get("random.count"));
        	if (conf.get("simple.count") != null)
        		simpleCount = Integer.parseInt((String)conf.get("simple.count"));
        	if (conf.get("stateful.count") != null)
        		statefulCount = Integer.parseInt((String)conf.get("stateful.count"));
        	if (conf.get("ack.count") != null)
        		ackCount = Integer.parseInt((String)conf.get("ack.count"));
        	if (conf.get("worker.count") != null)
        		workerCount = Integer.parseInt((String)conf.get("worker.count"));
        	if (conf.get("stage.count") != null)
        		stageCount = Integer.parseInt((String)conf.get("stage.count"));
        	if (conf.get("change.traffic.stage") != null)
        		changeTrafficStage = Integer.parseInt((String)conf.get("change.traffic.stage"));
        	if (conf.get("change.traffic.factor") != null)
        		changeTrafficFactor = Integer.parseInt((String)conf.get("change.traffic.factor"));
        }
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("random", new RandomSpout(), randomCount);
        String lastComponent = "random";
        int stage = 1;
        for (; stage < stageCount; stage++) {
        	int factor = 1;
        	if (stage == changeTrafficStage)
        		factor = changeTrafficFactor;
        	if (stage % 2 == 1) {
        		String previousComponent = null;
        		if (stage == 1)
        			previousComponent = "random";
        		else
        			previousComponent = (stage - 1) + "stateful";
        		builder.setBolt(stage + "simple", new SimpleBolt(factor), simpleCount).shuffleGrouping(previousComponent);
        		// builder.setBolt("simple-" + stage, new SimpleBolt(factor), simpleCount).localOrShuffleGrouping(previousComponent);
        		lastComponent = stage + "simple";
        	} else {
        		builder.setBolt(stage+"stateful", new StatefulBolt(factor), statefulCount).fieldsGrouping((stage - 1)+"simple", new Fields("random"));
        		lastComponent = stage+"stateful";
        	}
        }
        // builder.setBolt("ack", new AckBolt(), ackCount).directGrouping("stateful");
        if (stage % 2 == 0)
        	builder.setBolt("ack", new AckBolt(), ackCount).shuffleGrouping(lastComponent);
        else
        	builder.setBolt("ack", new AckBolt(), ackCount).fieldsGrouping(lastComponent, new Fields("random"));

        // data structures for offline scheduling
        List<String> components = new ArrayList<String>();
        components.add("random");
        for (stage = 1; stage < stageCount; stage++) {
        	if (stage % 2 == 1)
        		components.add(stage+"simple");
        	else
        		components.add(stage+"stateful");
        }
        components.add("ack");
        conf.put("components", components);
        
        Map<String, List<String>> streams = new HashMap<String, List<String>>();
        for (stage = 1; stage < stageCount; stage++) {
        	if (stage % 2 == 1) {
        		String previousComponent = null;
        		if (stage == 1)
        			previousComponent = "random";
        		else
        			previousComponent = (stage-1)+"stateful";
        		streams.put(stage+"simple", new ArrayList<String>()); 
        		streams.get(stage+"simple").add(previousComponent);
        	} else {
        		streams.put(stage+"stateful", new ArrayList<String>()); 
        		streams.get(stage+"stateful").add((stage - 1)+"simple");
        	}
        }
        if (stage % 2 == 0) {
        	streams.put("ack", new ArrayList<String>()); 
        	streams.get("ack").add((stage - 1)+"simple");
        } else {
        	streams.put("ack", new ArrayList<String>()); 
        	streams.get("ack").add((stage - 1)+"stateful");
        }
        conf.put("streams", streams);
        
        if (topologyName != null) {
        	conf.setNumWorkers(workerCount);
            conf.setMaxSpoutPending(100);
            conf.setNumAckers(0);
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
        	System.out.println("Running in local mode");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("reference", conf, builder.createTopology());
            Utils.sleep(30000);
            cluster.killTopology("reference");
            cluster.shutdown();
        }
	}
}
