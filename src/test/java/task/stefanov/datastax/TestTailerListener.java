package task.stefanov.datastax;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

public class TestTailerListener extends TailerListenerAdapter {

	private final List<String> lines = Collections.synchronizedList(new ArrayList<String>());

	volatile Exception exception = null;
	volatile int notFound = 0;
	volatile int rotated = 0;
	volatile int initialised = 0;
	volatile int reachedEndOfFile = 0;

	@Override
	public void handle(final String line) {
		lines.add(line);
	}

	public List<String> getLines() {
		return lines;
	}

	public void clear() {
		lines.clear();
	}

	@Override
	public void handle(final Exception e) {
		exception = e;
	}

	@Override
	public void init(final Tailer tailer) {
		initialised++; // not atomic, but OK because only updated here.
	}

	@Override
	public void fileNotFound() {
		notFound++; // not atomic, but OK because only updated here.
	}

	@Override
	public void fileRotated() {
		rotated++; // not atomic, but OK because only updated here.
	}

	@Override
	public void endOfFileReached() {
		reachedEndOfFile++; // not atomic, but OK because only updated here.
	}
}