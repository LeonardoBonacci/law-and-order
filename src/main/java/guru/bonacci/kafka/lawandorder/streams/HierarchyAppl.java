package guru.bonacci.kafka.lawandorder.streams;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;

import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;
import guru.bonacci.kafka.serialization.JacksonSerde;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HierarchyAppl {

	static final String DIGEST_STORE = "digest-store";
	static final String NODE_STORE = "nn-store";
	
	static final String DIGEST_TOPIC = "digested";
	static final String UNORDERED_TOPIC = "unordered";
	static final String ORDERED_TOPIC = "ordered";

	// https://dzone.com/articles/why-static-bad-and-how-avoid
	static ObjectMapper aum = new ObjectMapper();

	static ReadOnlyKeyValueStore<String, String> digestStore;

	public static void main(final String[] args) {
		final KafkaStreams streams = buildStream();
		streams.start();

		QueryableStoreType<ReadOnlyKeyValueStore<String, String>> queryableStoreType = QueryableStoreTypes.keyValueStore();
		digestStore = streams.store(StoreQueryParameters.fromNameAndType(DIGEST_STORE, queryableStoreType).enableStaleStores());

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	static KafkaStreams buildStream() {
		final Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "cool-app");
		config.put(StreamsConfig.CLIENT_ID_CONFIG, "hot-client");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
				LogAndFailExceptionHandler.class.getName());
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);

		// --------------------------------------------------------------------
        StreamsBuilder builder = new StreamsBuilder();
        
        // When all else fails, we can whip the horse's eyes And make them sleep, and cry.
		builder.table(DIGEST_TOPIC, Consumed.with(Serdes.String(), Serdes.String()),
				Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(DIGEST_STORE).withCachingDisabled());
        

        Serde<Node> nodeSerde = JacksonSerde.of(Node.class);
        Serde<NestedNode> nnodeSerde = JacksonSerde.of(NestedNode.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(NODE_STORE);

        StoreBuilder<KeyValueStore<String, NestedNode>> storeBuilder = Stores.keyValueStoreBuilder(
        		storeSupplier,
                Serdes.String(),
                nnodeSerde);

        builder.addStateStore(storeBuilder);

        Predicate<String, NodeWrapper> moveOnPredicate = (k, v) -> v.parentIsProcessed;
		Predicate<String, NodeWrapper> loopPredicate = (k, v) -> !moveOnPredicate.test(k, v);

		KStream<String, Node> unordered = 
			builder.stream(UNORDERED_TOPIC, Consumed.with(Serdes.String(), nodeSerde));
		unordered.peek((k,v) -> log.info(v.toString()));

        Predicate<String, NestedNode> hasChangedPredicate = (k, v) -> !hashMe(v).equals(digestStore.get(k));

		Consumer<KStream<String, NodeWrapper>> forward = moveOn -> {
			KStream<String, NestedNode> andOn = moveOn.mapValues((wrap) -> wrap.pnode)
						.filter(hasChangedPredicate)
			;
			
			andOn.to(ORDERED_TOPIC, Produced.with(Serdes.String(), nnodeSerde));
			andOn.mapValues(HierarchyAppl::hashMe).to(DIGEST_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
		};
		
		Consumer<KStream<String, NodeWrapper>> back = loopBack -> 
			loopBack.mapValues((wrap) -> wrap.pnode.toNode())
				  .to(UNORDERED_TOPIC, Produced.with(Serdes.String(), nodeSerde));
		
		unordered
			.transformValues(() -> new CrossRoadTransformer(NODE_STORE), NODE_STORE)
			.split()
			.branch(moveOnPredicate, Branched.withConsumer(forward))
			.branch(loopPredicate, Branched.withConsumer(back));
		// --------------------------------------------------------------------

		return new KafkaStreams(builder.build(), config);
	}
	
	static String hashMe(Object o) {
		try {
			return Hashing.sha256()
			  .hashString(aum.writeValueAsString(o), StandardCharsets.UTF_8)
			  .toString();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return "It is no measure of health to be well adjusted to a profoundly sick society";
		}
	}
}