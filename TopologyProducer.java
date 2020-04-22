package guru.bonacci.law.and.order.streams;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import guru.bonacci.law.and.order.model.Node;
import guru.bonacci.law.and.order.model.NodeWrapper;
import io.quarkus.kafka.client.serialization.JsonbSerde;

@ApplicationScoped
public class TopologyProducer {

	static final String STORE = "nodes-store";

    private static final String INPUT_TOPIC = "unordered-nodes";
    private static final String OUTPUT_TOPIC = "ordered-nodes";

	@SuppressWarnings("unchecked")
	@Produces
    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final JsonbSerde<Node> serde = new JsonbSerde<>(Node.class);

        final KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE);

        final StoreBuilder<KeyValueStore<String, Node>> storeBuilder = Stores.keyValueStoreBuilder(
        		storeSupplier,
                Serdes.String(),
                serde);

        builder.addStateStore(storeBuilder);

        final Predicate<String, NodeWrapper> moveOnPredicate = (k, v) -> v.parentIsProcessed;
		final Predicate<String, NodeWrapper> loopPredicate = (k, v) -> !moveOnPredicate.test(k, v);

		final KStream<String, Node> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), serde));
		
		final KStream<String, NodeWrapper>[] branched = sourceStream
							.transformValues(() -> new CrossRoadTransformer(STORE), STORE)
							.branch(moveOnPredicate, loopPredicate);
		// move on
		branched[0].mapValues(backToNormal).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), serde));
		// or back in line
        branched[1].mapValues(backToNormal).to(INPUT_TOPIC, Produced.with(Serdes.String(), serde));

        return builder.build();
    }
    
    ValueMapper<NodeWrapper, Node> backToNormal = (wrapper) -> wrapper.node;

    
    class CrossRoadTransformer implements ValueTransformerWithKey<String, Node, NodeWrapper> {

        private String storeName;
        private KeyValueStore<String, Node> store;

        public CrossRoadTransformer(String storeName) {
            this.storeName = storeName;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
        public void init(final ProcessorContext context) {
             store = (KeyValueStore) context.getStateStore(storeName);
        }

        @Override
        public NodeWrapper transform(final String key, final Node value) {
        	NodeWrapper nodew = new NodeWrapper(value);

            // As with every badly designed algorithm an exceptional case for the root.
            if (isEmpty(value.parentId)) {
            	// short-cut
                nodew.parentIsProcessed = true; 
                store.put(value.id, value);
                return nodew;
            }
            
        	// Parent at home..
            if ((store.get(value.parentId) != null)) {
                nodew.parentIsProcessed = true; 

                //..i need to leave a trace.
                store.put(value.id, value);
            } 

            return nodew;
        }

        @Override
        public void close() {
        	store.close();
        }
    }
}
