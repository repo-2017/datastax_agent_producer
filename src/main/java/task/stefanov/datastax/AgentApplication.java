package task.stefanov.datastax;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AgentApplication {

	private static final Logger logger = LogManager.getLogger(LogAggregationAgent.class);

	public static void main(String[] args) {
		logger.info("Starting Aggregation agent producer application ...");

		try {
			logger.info("Reading Agent application configuration.");
			Properties agentProps = PropsUtils.loadPropsFile(LogAggregationAgent.AGENT_CONFIG_NAME,
					LogAggregationAgent.DEFAULT_AGENT_CONF_NAME);
			String filepathsProp = (String) agentProps.get(LogAggregationAgent.TRACED_FILEPATHS_PROP);

			logger.info("Reading Kafka producer configuration properties.");
			Properties kafkaProps = PropsUtils.loadPropsFile(LogAggregationAgent.PRODUCER_CONFIG_NAME,
					LogAggregationAgent.DEFAULT_PRODUCER_CONF_NAME);

			// create, configure and start the agent
			final LogAggregationAgent agent = new LogAggregationAgent();
			agent.setLogFiles(filepathsProp.split(";")); // TODO validate file
			agent.setProducer(new KafkaProducer<String, String>(kafkaProps));

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					super.run();
					agent.getTailers().shutdown();
					try {
						agent.getTailers().awaitTermination(3, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
					} finally {
						if (agent.getProducer() != null)
							agent.getProducer().close();
					}
				}
			});

			agent.startTailing();
			
		} catch (IOException ex) {

			logger.fatal("Agent start failed !", ex);
		} finally {
		}
	}

}
