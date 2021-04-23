package guru.bonacci.kafka.lawandorder.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import guru.bonacci.kafka.lawandorder.model.Enlightened;
import guru.bonacci.kafka.lawandorder.model.Ignorant;
import guru.bonacci.kafka.lawandorder.model.IgnorantFlat;
import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.serialization.JacksonSerde;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProductAppl {

	static final String ORDERED_TOPIC = "ordered";
	static final String IGNORANT_TOPIC = "ignorant"; // lacking knowledge or awareness in general; uneducated or unsophisticated.
	static final String ENLIGHTENED_TOPIC = "enlightened"; // having or showing a rational, modern, and well-informed outlook.

	public static void main(final String[] args) {
		final KafkaStreams streams = buildStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	// https://www.youtube.com/watch?v=pQiYt58F_9k&ab_channel=KatiBreuer-Topic
	static KafkaStreams buildStream() {
		final Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "hot-app");
		config.put(StreamsConfig.CLIENT_ID_CONFIG, "cool-client");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				LogAndFailExceptionHandler.class.getName());
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);

		// --------------------------------------------------------------------
        StreamsBuilder builder = new StreamsBuilder();
        
		Serde<NestedNode> nnSerde = JacksonSerde.of(NestedNode.class);
		Serde<Ignorant> pSerde = JacksonSerde.of(Ignorant.class);
		Serde<IgnorantFlat> pafSerde = JacksonSerde.of(IgnorantFlat.class);
		Serde<Enlightened> rSerde = JacksonSerde.of(Enlightened.class);
		
		KTable<String, NestedNode> nnTable = 
			builder.table(ORDERED_TOPIC, Consumed.with(Serdes.String(), nnSerde));

		KTable<String, IgnorantFlat> igTable = 
			builder.stream(IGNORANT_TOPIC, Consumed.with(Serdes.String(), pSerde))
				.flatMapValues(Ignorant::flat)
				.peek((k,v) -> log.info("{}<ig>{}", k, v))
				.toTable(Materialized.<String, IgnorantFlat, KeyValueStore<Bytes, byte[]>>as("the-ignorant-schoolmaster")
	                        .withKeySerde(Serdes.String())
	                        .withValueSerde(pafSerde));

		igTable.join( nnTable, 
					   IgnorantFlat::getFkId, 
					   (ig, n) -> Enlightened.builder().ig(ig).nn(n).build())
				.toStream()
				.peek((k,v) -> log.info("{}<enl>{}", k, v))
				.filterNot((k,v) -> v == null)
				.to(ENLIGHTENED_TOPIC, Produced.with(Serdes.String(), rSerde));
		// --------------------------------------------------------------------

		return new KafkaStreams(builder.build(), config);
	}
}