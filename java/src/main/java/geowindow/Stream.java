package geowindow;

/**
 * Created by tom on 6/28/16.
 */

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class Stream {
    private final static String topicIn = "raw-tweets";
    private final static String topicOut = "win_tweet_counts";

    private static final Logger log = LoggerFactory.getLogger(Stream.class);

    public static void main(String[] args) throws Exception {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonWithEmptyDeser();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final String serial_number = Long.toString((new Date()).getTime());

        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-count-java-" + serial_number);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TweetTimestampExtractor.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Hexbin hexbin = new Hexbin(1.0/240.0);
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String,JsonNode> tweets = builder.stream(stringSerde, jsonSerde, topicIn);
        tweets
                .filterNot((k,v) -> v == null || v.get("geo").isNull())
                .map((k,v) -> {
                    JsonNode coords = v.get("geo").get("coordinates");
                    double[] bin = hexbin.bin(coords.get(0).doubleValue(),
                                              coords.get(1).doubleValue());
                    return KeyValue.pair(String.format("%f %f", bin[1], bin[0]), v);
                })
                .countByKey(TimeWindows.of("tweet-window", 60*60*1000), stringSerde)
                .toStream((k, v) -> String.format("%d %s", k.window().start(), k.key()))
                .to(stringSerde, longSerde, topicOut);

        KafkaStreams run = new KafkaStreams(builder, streamsConfiguration);
        run.start();
        Thread.sleep(50000);
        run.close();

    }

    private static class JsonWithEmptyDeser implements Deserializer<JsonNode> {
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

        @Override
        public void configure(Map<String, ?> map, boolean b) {
            jsonDeserializer.configure(map, b);
        }

        @Override
        public JsonNode deserialize(String s, byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return null;
            } else {
                try {
                    return jsonDeserializer.deserialize(s, bytes);
                } catch (SerializationException e) {
                    try {
                        log.warn("Exception during deserialization of string:\n" + (new String(bytes, "UTF-8")), e);
                    } catch (UnsupportedEncodingException uee) {}
                    return null;
                }
            }
        }

        @Override
        public void close() {
            jsonDeserializer.close();
        }
    }

    public static class TweetTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> consumerRecord) {
            Object val = consumerRecord.value();

            if (val == null)
                return 0;
            else
                return ((JsonNode) val).get("timestamp_ms").asLong();
        }
    }
}
