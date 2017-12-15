package task.stefanov.datastax;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class LogAggregationAgentTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File getTestDirectory() {
		return temporaryFolder.getRoot();
	}

	@Test
	public void startTailingTest() throws Exception {
		final File testFile = new File(getTestDirectory(), "unittest.log");
		final String[] testLines = new String[] { "test log line 1", "another test log line 2" };
		write(testFile, "begin");

		// prepare agent to tail a test file
		LogAggregationAgent agent = new LogAggregationAgent();
		MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(),
				new StringSerializer());

		agent.setLogFiles(new String[] { testFile.getAbsolutePath() });
		agent.setProducer(mockProducer);

		agent.startTailing();

		Thread.sleep(50l);
		write(testFile, testLines);
		Thread.sleep(50l);

		assertTrue("Expected message count doesn't match !", mockProducer.history().size() == 2);

		assertEquals("Sent message doesn't match the logged one !", mockProducer.history().get(0).value(),
				testLines[0]);
		assertEquals("Sent message doesn't match the logged one !", mockProducer.history().get(1).value(),
				testLines[1]);

	}

	private void write(final File file, final String... lines) throws Exception {
		FileWriter writer = null;
		try {
			writer = new FileWriter(file, true);
			for (final String line : lines) {
				writer.write(line + "\n");
			}
		} finally {
			if (writer != null) {
				writer.flush();
				writer.close();
			}
		}
	}

}
