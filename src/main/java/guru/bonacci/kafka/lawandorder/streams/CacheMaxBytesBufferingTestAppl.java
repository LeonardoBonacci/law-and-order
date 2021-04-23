package guru.bonacci.kafka.lawandorder.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

public class CacheMaxBytesBufferingTestAppl {

	public static void main(final String[] args) {
		final KafkaStreams streams = buildStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	static KafkaStreams buildStream() {
		final Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, Uuid.randomUuid().toString());
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				LogAndFailExceptionHandler.class.getName());

		// --------------------------------------------------------------------
        StreamsBuilder builder = new StreamsBuilder();
        
		KTable<String, String> right = 
			builder.table("right", Consumed.with(Serdes.String(), Serdes.String()));

		KTable<String, String> left = 
			builder.stream("left", Consumed.with(Serdes.String(), Serdes.String()))
				.flatMapValues(v -> Arrays.asList(v, v))
				.toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("foo")
	                        .withKeySerde(Serdes.String())
	                        .withValueSerde(Serdes.String()));

		left.join( right,
				   str -> str, // lvalue on rkey
				   (l, r) -> l.toString() + r.toString())
				.toStream()
				.peek((k,v) -> System.out.println(k + "<>" + v))
				.filterNot((k,v) -> v == null)
				.print(Printed.toSysOut());
;
		// --------------------------------------------------------------------

		return new KafkaStreams(builder.build(), config);
	}
}