package org.acme.kafka.lawandorder.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;
import guru.bonacci.kafka.lawandorder.model.PathNode;
import guru.bonacci.kafka.serialization.JacksonSerde;

@ApplicationScoped
public class TopologyProducer {

	static final String STORE = "store";

    private static final String INPUT_TOPIC = "unordered";
    private static final String OUTPUT_TOPIC = "ordered";

	@SuppressWarnings("unchecked")
	@Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Node> nodeSerde = JacksonSerde.of(Node.class);
        Serde<PathNode> pnodeSerde = JacksonSerde.of(PathNode.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE);

        StoreBuilder<KeyValueStore<String, PathNode>> storeBuilder = Stores.keyValueStoreBuilder(
        		storeSupplier,
                Serdes.String(),
                pnodeSerde);

        builder.addStateStore(storeBuilder);

        Predicate<String, NodeWrapper> moveOnPredicate = (k, v) -> v.parentIsProcessed;
		Predicate<String, NodeWrapper> loopPredicate = (k, v) -> !moveOnPredicate.test(k, v);

		KStream<String, Node> unordered = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), nodeSerde));
		
		KStream<String, NodeWrapper>[] branched = unordered
							.transformValues(() -> new CrossRoadTransformer(STORE), STORE)
							.branch(moveOnPredicate, loopPredicate);
		// move on
		branched[0]
			.mapValues((wrap) -> wrap.pnode)
        	.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), pnodeSerde));

		// loop back
        branched[1]
        	.mapValues((wrap) -> wrap.pnode.toNode())
        	.to(INPUT_TOPIC, Produced.with(Serdes.String(), nodeSerde));

        return builder.build();
    }
}