# SBS-1 (BaseStation) Kafka Source Connector

This repository contains a Kafka Source Connector for SBS-1 (ADS-B) messages. The messages are parsed from a decoded feed such as produced by [dump1090](https://github.com/antirez/dump1090) into JSON and pushed into a Kafka cluster.  The Kafka messages are keyed using the message type, transmission type and hex identifier. For example, here is a list of messages and their generated keys.

| Message (truncated) | Key |
| --- | --- |
| ``AIR,,333,272,AD2328,372,2020/07/21,18:05:22.301,2020/07/21,18:05:2...`` | ``AIR--AD2328``  |
| ``ID,,333,272,AD2328,372,2020/07/21,18:05:22.301,2020/07/21,18:05:22...`` | ``ID--AD2328``   |
| ``MSG,3,333,272,AD2328,372,2020/07/21,18:05:22.417,2020/07/21,18:05:...`` | ``MSG-3-AD2328`` |
| ``MSG,4,333,272,AD2328,372,2020/07/21,18:05:22.417,2020/07/21,18:05:...`` | ``MSG-4-AD2328`` |

Only the fields associated with each specific message type are included in the generated JSON. The following is an example JSON message published to Kafka.

```json
{
    "MT": "MSG",
    "TT": "3",
    "SID": "333",
    "AID": "485",
    "HEX": "A2B081",
    "FID": "585",
    "DMG": "2020/07/23",
    "TMG": "15:46:49.878",
    "DML": "2020/07/23",
    "TML": "15:46:49.878",
    "ALT": "23325",
    "LAT": "40.65258",
    "LNG": "-75.60273",
    "ALRT": "0",
    "EMER": "0",
    "SPI": "0",
    "GND": "0"
}
```

### Build

This project uses the standard maven wrapper to build using Maven. Please adjust the following command for your environment.

```
./mvnw clean package
```

### Configure

All configuration is encapsulated in the standard Kafka Connect properties file [Sbs1SourceConnector.properties](config/Sbs1SourceConnector.properties). The properties of most interest are

* **host**: Hostname or IP address of the SBS-1 feed (default: 127.0.0.1)
* **port**: Port of the decoded SBS-1 feed (default: 30003)
* **topic**: Kafka topic to publish messages to (default: dump1090)
* **batch.size**: Batch size to publish to Kafka (default: 100)

### Running

You must have both a decoded SBS-1 feed (like from dump1090) and a Kafka instance available. Replace the respective hostname or IP address values in the configuration file [Sbs1SourceConnector.properties](config/Sbs1SourceConnector.properties) to have Kafka Connect start receiving SBS-1 messages and generating Kafka messages.

```
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/Sbs1SourceConnector.properties
```

You can see the JSON produced by using the Kafka console consumer. Replace the respective hostname or IP address value in the following command to connect to Kafka and view the messages from the default topic name.

```
kafka-console-consumer --bootstrap-server Y.Y.Y.Y:9092 --topic dump1090 --from-beginning
```

### Future Work

1. Add tests
1. Make the key configurable with a template

### References

* http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm
* https://github.com/antirez/dump1090
* https://www.confluent.io/blog/noise-mapping-ksql-raspberry-pi-software-defined-radio/
* https://eventador.io/blog/planestream-the-ads-b-datasource/
* https://github.com/GuyIEX/sbs1_kafka_gateway
