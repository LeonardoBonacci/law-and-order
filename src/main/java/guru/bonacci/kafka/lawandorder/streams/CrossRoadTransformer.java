package guru.bonacci.kafka.lawandorder.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CrossRoadTransformer implements ValueTransformerWithKey<String, Node, NodeWrapper> {

	private String storeName;
	private KeyValueStore<String, NestedNode> store;
	private long lastProcessedRecordTime;

	public CrossRoadTransformer(String storeName) {
		this.storeName = storeName;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void init(final ProcessorContext contextus) {
		contextus.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, this::punctuator);
		store = (KeyValueStore) contextus.getStateStore(storeName);
	}

	@Override
	public NodeWrapper transform(final String ignoreKey, final Node node) {
		NodeWrapper wrap = new NodeWrapper(); 
		wrap.pnode = NestedNode.from(node);

		// As with every mediocre algorithm an exceptional case for the root
		if (node.parentId == null || node.parentId.isBlank()) {
			wrap.parentIsProcessed = true;
			store.put(node.id, wrap.pnode);
			return wrap;
		}

		// Parent at home..
		NestedNode parent = store.get(node.parentId);
		if (parent != null) {
			wrap.parentIsProcessed = true;
			wrap.pnode.parent = parent;

			// ..leave a trace
			store.put(node.id, wrap.pnode);
		}

		lastProcessedRecordTime = System.currentTimeMillis();
		return wrap;
	}

	@Override
	public void close() {
		store.close();
	}

	void punctuator(Long timestamp) {
		Duration diff = Duration.between(
				Instant.ofEpochMilli(lastProcessedRecordTime),
				Instant.ofEpochMilli(timestamp));
		log.warn("Punctuator diff in seconds {}", diff.getSeconds());

		if (diff.getSeconds() < 15) {
			log.info("@{} Skip state store reset", new Date(timestamp));
			return;
		}

		try (KeyValueIterator<String, NestedNode> it = store.all()) {
			while (it.hasNext()) {
				store.put(it.next().key, null);
			}
		}
		log.info("@{} Reset state store", new Date(timestamp));
	}
}