package task.stefanov.datastax;

import java.io.File;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author stefanov
 *
 */
public class LogTailerListener implements TailerListener {

	private static final Logger logger = LogManager.getLogger(LogTailerListener.class);

	private final String filepath;
	private final File file;
	private final Producer<String, String> producer;

	public LogTailerListener(String filepath, File file, Producer<String, String> producer) {
		this.filepath = filepath;
		this.file = file;
		this.producer = producer;
	}

	@Override
	public void init(Tailer arg0) {
	}

	@Override
	public void handle(Exception ex) {
		logger.error("Tailer failed for log: " + filepath, ex);
	}

	@Override
	public void handle(String line) {
		if (logger.isDebugEnabled())
			logger.debug("Sending message : " + file.getName() + " | " + line);
		producer.send(new ProducerRecord<String, String>(file.getName(), line));
	}

	@Override
	public void fileRotated() {
	}

	@Override
	public void fileNotFound() {
		logger.error("Could not find log file: " + filepath);
	}
}
