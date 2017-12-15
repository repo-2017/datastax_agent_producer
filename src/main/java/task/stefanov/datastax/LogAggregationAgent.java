package task.stefanov.datastax;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author stefanov
 */
public class LogAggregationAgent {

	private static final Logger logger = LogManager.getLogger(LogAggregationAgent.class);

	public static final String TRACED_FILEPATHS_PROP = "traced.filepaths";
	public static final String DEFAULT_AGENT_CONF_NAME = "default-agent.props";
	public static final String AGENT_CONFIG_NAME = "agent.props";
	public static final String PRODUCER_CONFIG_NAME = "producer.props";
	public static final String DEFAULT_PRODUCER_CONF_NAME = "default-producer.props";

	private final ExecutorService tailers = Executors.newCachedThreadPool();
	private Producer<String, String> producer;
	private String[] logFiles;

	public LogAggregationAgent() {
	}

	public LogAggregationAgent(String[] logFiles, Producer<String, String> producer) {
		this.logFiles = logFiles;
		this.producer = producer;
	}

	/**
	 * 
	 */
	public void startTailing() {
		logger.info("Initiating tailing for configured log files.");

		Arrays.stream(logFiles).forEach(filepath -> initiateTailing(filepath));
	}

	/**
	 * 
	 * @param filepath
	 * @throws URISyntaxException
	 */
	private void initiateTailing(final String filepath) {
		File file = new File(filepath);

		if (file.exists()) {
			logger.info("Initiating tailing for log: " + filepath);

			TailerListener listener = new LogTailerListener(filepath, file, getProducer());
			Tailer tailer = new Tailer(file, listener, 10, true, true);
			Thread tailerRunnable = new Thread(tailer);

			tailers.submit(tailerRunnable);
		} else {

			logger.error("Could not find log file: " + filepath);
		}
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public String[] getLogFiles() {
		return logFiles;
	}

	public void setLogFiles(String[] logFiles) {
		this.logFiles = logFiles;
	}

	public ExecutorService getTailers() {
		return tailers;
	}
}
