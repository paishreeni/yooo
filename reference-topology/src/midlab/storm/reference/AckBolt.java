package midlab.storm.reference;

import java.util.Map;

import midlab.storm.monitor.LatencyThroughputManager;
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class AckBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8743183987805808152L;
	
	private TaskMonitor taskMonitor;
	private LatencyThroughputManager latencyThroughputManager;
	private Logger logger;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		WorkerMonitor.getInstance().setContextInfo(context);
		taskMonitor = new TaskMonitor(context.getThisTaskId());
		logger = Logger.getLogger(StatefulBolt.class);
		try {
			latencyThroughputManager = new LatencyThroughputManager("ack", context.getThisTaskId(), stormConf, context, true, true);
		} catch (Exception e) {
			logger.error("Error creating the latencyThroughputManager", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		taskMonitor.notifyTupleReceived(input);
		latencyThroughputManager.notifyEventCompleted(System.currentTimeMillis() - input.getLong(1));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
