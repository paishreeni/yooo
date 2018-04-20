package midlab.storm.reference;

import java.util.Map;

import midlab.storm.monitor.ProcessingTimeManager;
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8743183987805808152L;
	
	private OutputCollector collector;
	private TaskMonitor taskMonitor;
	private ProcessingTimeManager processingTimeManager;
	private int loops;
	private int preferredValue;
	private int value;
	private boolean flipFlop;
	private Logger logger;
	private int changeTrafficFactor;
	int counter;
	
	public SimpleBolt(int changeTrafficFactor) {
		this.changeTrafficFactor = changeTrafficFactor;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		WorkerMonitor.getInstance().setContextInfo(context);
		if (stormConf.get("preferred.value") != null) {
			int exp = Integer.parseInt((String)stormConf.get("preferred.value"));
			preferredValue = (int)Math.pow(context.getThisTaskId(), exp);
		}
		value = context.getThisTaskId();
		taskMonitor = new TaskMonitor(context.getThisTaskId());
		logger = Logger.getLogger(SimpleBolt.class);
		loops = Integer.parseInt((String)stormConf.get("loops"));
		try {
			processingTimeManager = new ProcessingTimeManager("simple", context.getThisTaskId(), stormConf, context);
		} catch (Exception e) {
			logger.error("Error creating the ProcessingTimeManager", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		taskMonitor.notifyTupleReceived(input);
		long begin = System.nanoTime();
		for (int i = 0; i < loops; i++);
		int valueToEmit;
		if (preferredValue == 0) {
			valueToEmit = value++;
		} else {
			if (flipFlop)
				valueToEmit = value++;
			else
				valueToEmit = preferredValue;
			flipFlop = ! flipFlop;
		}
		
		if (changeTrafficFactor > 0) {
			for (int i = 0; i < changeTrafficFactor; i++)
				collector.emit(new Values(valueToEmit, input.getLong(1)));
		} else if (++counter == -changeTrafficFactor) {
			counter = 0;
			collector.emit(new Values(valueToEmit, input.getLong(1)));
		}
		long end = System.nanoTime();
		processingTimeManager.notifyProcessingTime((end - begin) / 1000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("random", "latency-ts"));
	}

}
