package midlab.storm.reference;

import java.util.Map;

import midlab.storm.monitor.EventProducer;
import midlab.storm.monitor.EventRateController;
import midlab.storm.monitor.InterNodeTrafficManager;
import midlab.storm.monitor.LatencyThroughputManager;
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSpout extends BaseRichSpout implements EventProducer {
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	// private Random rand;
	private int value;
	private TaskMonitor taskMonitor;
	private LatencyThroughputManager latencyThroughputManager;
	@SuppressWarnings("unused") private InterNodeTrafficManager interNodeTrafficManager;
	private EventRateController eventRateController;
	private Logger logger;
	private int packetRate;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(RandomSpout.class);
		// rand = new Random(System.currentTimeMillis());
		value = context.getThisTaskId();
		WorkerMonitor.getInstance().setContextInfo(context);
		taskMonitor = new TaskMonitor(context.getThisTaskId());
		
		try {
			latencyThroughputManager = new LatencyThroughputManager("random", context.getThisTaskId(), conf, context, false,  true);
			
			// check whether this is the task in charge of monitoring inter-node traffic
			if (context.getThisTaskId() == context.getComponentTasks(context.getThisComponentId()).get(0))
				interNodeTrafficManager = new InterNodeTrafficManager(conf, context);
		} catch (Exception e) {
			logger.error("Error creating LatencyThroughputManager or InterNodeTrafficManager", e);
		}
		
		packetRate = Integer.parseInt((String)conf.get("input.throughput"));
		int burstPeriod = Integer.parseInt((String)conf.get("burst.period"));
		if (packetRate > 0) {
			if (conf.get("input.throughput.variance") != null && context.getComponentTasks(context.getThisComponentId()).size() > 1) {
				int variance = Integer.parseInt((String)conf.get("input.throughput.variance")); // in %
				int varianceInterval = (packetRate / 100) * variance * 2;
				int taskVarianceSlot = varianceInterval / (context.getComponentTasks(context.getThisComponentId()).size() - 1);
				packetRate += - varianceInterval/2 + context.getThisTaskIndex() * taskVarianceSlot;
				logger.info("Apply variance " + variance + "% to packet rate, new packet rate is " + packetRate);
			}
			eventRateController = new EventRateController(this, packetRate, burstPeriod);
		}
	}
	
	@Override
	public void nextTuple() {
		if (eventRateController != null) {
			eventRateController.control();
		} else {
			if (packetRate < 0)
				Utils.sleep(-packetRate);
			produce();
		}
	}

	@Override
	public void produce() {
		taskMonitor.checkThreadId();
		// collector.emit(new Values(rand.nextInt(100), System.currentTimeMillis()));
		collector.emit(new Values(value++, System.currentTimeMillis()));
		latencyThroughputManager.notifyEventCompleted(0);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("random", "latency-ts"));
	}
}
