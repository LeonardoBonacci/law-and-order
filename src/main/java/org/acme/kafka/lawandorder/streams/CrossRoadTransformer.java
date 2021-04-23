package org.acme.kafka.lawandorder.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;

class CrossRoadTransformer implements ValueTransformerWithKey<String, Node, NodeWrapper> {

	private String storeName;
	private KeyValueStore<String, Node> store;
	private long lastProcessedRecordTime;
	
	public CrossRoadTransformer(String storeName) {
		this.storeName = storeName;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void init(final ProcessorContext contextus) {
		contextus.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::punctuator);
		store = (KeyValueStore) contextus.getStateStore(storeName);
	}

	@Override
	public NodeWrapper transform(final String key, final Node value) {
		NodeWrapper nodew = NodeWrapper.from(value);

		// As with every mediocre algorithm an exceptional case for the root
		if (value.parentId == null || value.parentId.isBlank()) {
			nodew.parentIsProcessed = true;
			store.put(value.id, value);
			return nodew;
		}

		// Parent at home..
		if ((store.get(value.parentId) != null)) {
			nodew.parentIsProcessed = true;

			// ..leave a trace.
			store.put(value.id, value);
		}

		lastProcessedRecordTime = System.currentTimeMillis();
		return nodew;
	}

	@Override
	public void close() {
		store.close();
	}

	void punctuator(Long timestamp) {
		Duration diff = Duration.between(Instant.ofEpochMilli(lastProcessedRecordTime), Instant.ofEpochMilli(timestamp));
		System.out.println("Punctuator diff in seconds " + diff.getSeconds());

		if (diff.getSeconds() < 2) {
			System.out.println("@" + new Date(timestamp) + " Skip state store reset");
			return;
		}

		try (KeyValueIterator<String, Node> iter = store.all()) {
			while (iter.hasNext()) {
				store.put(iter.next().key, null);
			}
		}
		System.out.println("@" + new Date(timestamp) + " Reset state store");
	}
}