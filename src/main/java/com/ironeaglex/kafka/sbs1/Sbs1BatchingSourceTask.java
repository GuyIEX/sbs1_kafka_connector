/**
 * sbs1_kafka_connector
 * Copyright (C) 2020  Iron EagleX
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.ironeaglex.kafka.sbs1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sbs1BatchingSourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(Sbs1BatchingSourceTask.class);
	public static final String HOST_FIELD = "host";
	public static final String POSITION_FIELD = "position";
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

	private String host;
	private int port = Sbs1SourceConnector.DEFAULT_PORT;
	private InputStream stream;
	private BufferedReader reader = null;
	private char[] buffer = new char[1024];
	private int offset = 0;
	private String topic = null;

	private Socket source;

	private int batchSize = Sbs1SourceConnector.DEFAULT_TASK_BATCH_SIZE;

	private static final String SBS1_MSG_JSON_TEMPLATE =
		"{\"messageType\":\"%1$s\"," +
		"\"transmissionType\":\"%2$s\"," +
		"\"sessionId\":\"%3$s\"," +
		"\"aircraftId\":\"%4$s\"," +
		"\"hexIdent\":\"%5$s\"," +
		"\"flightId\":\"%6$s\"," +
		"\"generatedDate\":\"%7$s\"," +
		"\"generatedTime\":\"%8$s\"," +
		"\"loggedDate\":\"%9$s\"," +
		"\"loggedTime\":\"%10$s\"," +
		"\"callsign\":\"%11$s\"," +
		"\"altitude\":\"%12$s\"," +
		"\"groundSpeed\":\"%13$s\"," +
		"\"track\":\"%14$s\"," +
		"\"latitude\":\"%15$s\"," +
		"\"longitude\":\"%16$s\"," +
		"\"verticalRate\":\"%17$s\"," +
		"\"squawk\":\"%18$s\"," +
		"\"alert\":\"%19$s\"," +
		"\"emergency\":\"%20$s\"," +
		"\"spi\":\"%21$s\"," +
		"\"isOnGround\":\"%22$s\"}";

	@Override
	public String version() {
		return new Sbs1SourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		host = props.get(Sbs1SourceConnector.HOST_CONFIG);
		if (host == null || host.isEmpty()) {
			stream = System.in;
			// Tracking offset for stdin doesn't make sense
			reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		}
		port = Integer.parseInt(props.get(Sbs1SourceConnector.PORT_CONFIG));
		// Missing topic or parsing error is not possible because we've parsed the
		// config in the Connector
		topic = props.get(Sbs1SourceConnector.TOPIC_CONFIG);
		batchSize = Integer.parseInt(props.get(Sbs1SourceConnector.TASK_BATCH_SIZE_CONFIG));
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		if (stream == null) {
			try {
				source = new Socket(host, port);
				stream = source.getInputStream();
				reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				log.debug("Connected to {} for reading", logConnectionName());
			} catch (UnknownHostException e) {
				log.error("Couldn't find the host {}: ", logConnectionName(), e);
				throw new ConnectException(e);
			} catch (IOException e) {
				log.error("Error while trying to access {}: ", logConnectionName(), e);
				throw new ConnectException(e);
			}
		}

		// Unfortunately we can't just use readLine() because it blocks in an
		// uninterruptible way.
		// Instead we have to manage splitting lines ourselves, using simple backoff
		// when no new data
		// is available.
		try {
			final BufferedReader readerCopy;
			synchronized (this) {
				readerCopy = reader;
			}
			if (readerCopy == null)
				return null;

			ArrayList<SourceRecord> records = null;

			int nread = 0;
			while (readerCopy.ready()) {
				nread = readerCopy.read(buffer, offset, buffer.length - offset);
				log.trace("Read {} bytes from {}", nread, logConnectionName());

				if (nread > 0) {
					offset += nread;
					if (offset == buffer.length) {
						char[] newbuf = new char[buffer.length * 2];
						System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
						buffer = newbuf;
					}

					String line;
					do {
						line = extractLine();
						if (line != null) {
							log.trace("Read a line from {}", logConnectionName());
							if (records == null) {
								records = new ArrayList<>();
							}

							// Convert line to JSON
							// Get rid of any CRLF characters on the end of the string
							line = line.trim();

							// Ensure there are 22 fields for the template
							Object[] parts = new Object[22];
							Arrays.fill(parts, "");
							Object[] temp = line.split(",");
							System.arraycopy(temp, 0, parts, 0, temp.length);

							// Build the payload from the template
							line = String.format(SBS1_MSG_JSON_TEMPLATE, parts);

							// Build a key of the message type, transmission type and hex identifier
							String key = parts[0]+"-"+parts[1]+"-"+parts[4];

							records.add(new SourceRecord(offsetKey(logConnectionName()), offsetValue(System.currentTimeMillis()),
									topic, null, KEY_SCHEMA, key, VALUE_SCHEMA, line, System.currentTimeMillis()));

							if (records.size() >= batchSize) {
								return records;
							}
						}
					} while (line != null);
				}
			}

			if (nread <= 0) {
				synchronized (this) {
					this.wait(1000);
				}
			}

			return records;
		} catch (IOException e) {
			// Underlying stream was killed, probably as a result of calling stop. Allow to
			// return
			// null, and driving thread will handle any shutdown if necessary.
		}
		return null;
	}

	private String extractLine() {
		int until = -1, newStart = -1;
		for (int i = 0; i < offset; i++) {
			if (buffer[i] == '\n') {
				until = i;
				newStart = i + 1;
				break;
				// } else if (buffer[i] == '\r') {
				// // We need to check for \r\n, so we must skip this if we can't check the next
				// char
				// if (i + 1 >= offset)
				// return null;

				// until = i;
				// newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
				// break;
			}
		}

		if (until != -1) {
			String result = new String(buffer, 0, until);
			System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
			offset = offset - newStart;
			// if (streamOffset != null)
			// streamOffset += newStart;
			return result;
		} else {
			return null;
		}
	}

	@Override
	public void stop() {
		log.trace("Stopping");
		synchronized (this) {
			try {
				if (stream != null && stream != System.in) {
					stream.close();
					log.trace("Closed input stream");
				}
			} catch (IOException e) {
				log.error("Failed to close Sbs1SourceTask stream: ", e);
			}
			try {
				if (source != null) {
					source.close();
					log.trace("Closed input socket");
				}
			} catch (IOException e) {
				log.error("Failed to close Sbs1SourceTask socket: ", e);
			}
			this.notify();
		}
	}

	private Map<String, String> offsetKey(String host) {
		return Collections.singletonMap(HOST_FIELD, host);
	}

	private Map<String, Long> offsetValue(Long pos) {
		return Collections.singletonMap(POSITION_FIELD, pos);
	}

	private String logConnectionName() {
		return host == null ? "stdin" : host + ":" + port;
	}

}
