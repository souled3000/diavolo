package ind.diavolo.demo.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class B1 implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -473235171303832091L;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		String[] s = input.getString(0).split("[|]");
		String dv = s[2];
		String lt = s[3];
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
