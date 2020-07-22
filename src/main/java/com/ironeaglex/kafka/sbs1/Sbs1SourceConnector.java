/**
 *
 */
package com.ironeaglex.kafka.sbs1;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Based loosely on
 * https://github.com/apache/kafka/blob/trunk/connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSourceConnector.java
 */
public class Sbs1SourceConnector extends SourceConnector {

	public static final String HOST_CONFIG = "host";
	public static final String PORT_CONFIG = "port";
	public static final String TOPIC_CONFIG = "topic";
	public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

	public static final int DEFAULT_PORT = 30003;
	public static final int DEFAULT_TASK_BATCH_SIZE = 100;

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(HOST_CONFIG, Type.STRING, null, Importance.HIGH, "Hostname or IP address of the SBS-1 source feed")
			.define(PORT_CONFIG, Type.INT, DEFAULT_PORT, Importance.HIGH, "Port of the SBS-1 source feed")
			.define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
			.define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
					"The maximum number of records the Source task can read from the feed at a time");

	private String host;
	private int port;
	private String topic;
	private int batchSize;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
		host = parsedConfig.getString(HOST_CONFIG);
		port = parsedConfig.getInt(PORT_CONFIG);
		List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
		if (topics.size() != 1) {
			throw new ConfigException("'topic' in " + this.getClass().getSimpleName()
					+ " configuration requires definition of a single topic");
		}
		topic = topics.get(0);
		batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return Sbs1BatchingSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		// Only one input stream makes sense.
		Map<String, String> config = new HashMap<>();
		if (host != null)
			config.put(HOST_CONFIG, host);
		config.put(PORT_CONFIG, String.valueOf(port));
		config.put(TOPIC_CONFIG, topic);
		config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		// Nothing to do since FileStreamSourceConnector has no background monitoring.
		// TODO: Add background monitoring?
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
