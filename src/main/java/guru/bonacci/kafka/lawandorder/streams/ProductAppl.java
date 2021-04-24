package guru.bonacci.kafka.lawandorder.streams;

import static guru.bonacci.kafka.lawandorder.domain.Schemas.IGNORANT_FLAT_VALUE_SERDE;
import static guru.bonacci.kafka.lawandorder.domain.Schemas.Topics.ENLIGHTENED;
import static guru.bonacci.kafka.lawandorder.domain.Schemas.Topics.IGNORANT;
import static guru.bonacci.kafka.lawandorder.domain.Schemas.Topics.ORDERED;

import java.util.Properties;

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

import guru.bonacci.kafka.lawandorder.domain.Schemas;
import guru.bonacci.kafka.lawandorder.model.Enlightened;
import guru.bonacci.kafka.lawandorder.model.Ignorant;
import guru.bonacci.kafka.lawandorder.model.IgnorantFlat;
import guru.bonacci.kafka.lawandorder.model.NestedNode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProductAppl {

	public static void main(final String[] args) {
		final KafkaStreams streams = buildStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	
	// https://www.youtube.com/watch?v=pQiYt58F_9k&ab_channel=KatiBreuer-Topic
	static KafkaStreams buildStream() {
		final String schemaRegistryUrl = null;
		Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
		
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
        
		KTable<String, NestedNode> nnTable = 
			builder.table(ORDERED.name(), Consumed.with(ORDERED.keySerde(), ORDERED.valueSerde()));

		KTable<String, IgnorantFlat> igTable = 
			builder.stream(IGNORANT.name(), Consumed.with(IGNORANT.keySerde(), IGNORANT.valueSerde()))
				.flatMapValues(Ignorant::flat)
				.peek((k,v) -> log.info("{}<ig>{}", k, v))
				.toTable(Materialized.<String, IgnorantFlat, KeyValueStore<Bytes, byte[]>>as("the-ignorant-schoolmaster")
	                        .withKeySerde(Serdes.String())
	                        .withValueSerde(IGNORANT_FLAT_VALUE_SERDE));

		igTable.join( nnTable, 
					   IgnorantFlat::getFkId, 
					   Enlightened::new)
				.toStream()
				.peek((k,v) -> log.info("{}<enl>{}", k, v))
				.filterNot((k,v) -> v == null)
				.to(ENLIGHTENED.name(), Produced.with(ENLIGHTENED.keySerde(), ENLIGHTENED.valueSerde()));
		// --------------------------------------------------------------------

		return new KafkaStreams(builder.build(), config);
	}
}