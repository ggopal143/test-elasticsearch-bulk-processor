package com.github.richardwilly98;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.indexRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.base.Stopwatch;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

public class TestBulkProcessor {

	private static final ESLogger logger = Loggers
			.getLogger(TestBulkProcessor.class);
	public final static int DEFAULT_CONCURRENT_REQUESTS = 50;
	public final static int DEFAULT_BULK_ACTIONS = 1000;
	public final static TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue
			.timeValueMillis(10);
	public final static ByteSizeValue DEFAULT_BULK_SIZE = new ByteSizeValue(5,
			ByteSizeUnit.MB);

	// private final BulkProcessor bulkProcessor;
	private BulkProcessor bulkProcessor;
	private final TransportClient client;
	private static final String INDEX_NAME = "dummy-"
			+ System.currentTimeMillis();
	private static final String TYPE_NAME = "my-type";
	private static final long MAX = 1000000;

	private AtomicInteger indexedDocumentCount = new AtomicInteger();

	public TestBulkProcessor(TransportClient client) {
		this.client = client;
		logger.info("Available processor: {}", Runtime.getRuntime()
				.availableProcessors());
		// this.bulkProcessor = BulkProcessor.builder(client, listener)
		// .setBulkActions(DEFAULT_BULK_ACTIONS)
		// .setConcurrentRequests(Runtime.getRuntime().availableProcessors())
		// // .setFlushInterval(DEFAULT_FLUSH_INTERVAL)
		// .setBulkSize(DEFAULT_BULK_SIZE)
		// .build();
	}

	public static void main(String[] args) {
		TransportClient client = new TransportClient();
		client.addTransportAddress(new InetSocketTransportAddress("localhost",
				9300));
		TestBulkProcessor test = new TestBulkProcessor(client);
		int count = 0;
		int processors = Runtime.getRuntime().availableProcessors() - 1;
		while (count++ < 20) {
			int count2 = 0;
			while (count2++ < 3) {
				test.testBulkProcessor(processors);
			}
			processors++;
		}
		client.close();
	}

	void testBulkProcessor(int processors) {
		logger.info("Test bulk processor with concurrent request: {}", processors);
		Stopwatch watcher = Stopwatch.createStarted();
		try {
			indexedDocumentCount.set(0);
			bulkProcessor = BulkProcessor.builder(client, listener)
					.setBulkActions(DEFAULT_BULK_ACTIONS)
					.setConcurrentRequests(processors)
					// .setFlushInterval(DEFAULT_FLUSH_INTERVAL)
					.setBulkSize(DEFAULT_BULK_SIZE).build();
			if (client.admin().indices().prepareExists(INDEX_NAME).get()
					.isExists()) {
				client.admin().indices().prepareDelete(INDEX_NAME).get();
			}
			client.admin().indices().prepareCreate(INDEX_NAME).get();
			logger.info(
					"Done Cluster Health, status: {}",
					client.admin()
							.cluster()
							.health(clusterHealthRequest().waitForGreenStatus())
							.get().getStatus());
			for (int i = 0; i < MAX; i++) {
				Map<String, Object> data = new HashMap<String, Object>();
				data.put("name", "test-" + i);
				data.put("date", new Date());
				bulkProcessor.add(indexRequest(INDEX_NAME).type(TYPE_NAME)
						.source(XContentFactory.jsonBuilder().map(data)));
			}
			bulkProcessor.close();
			logger.info(
					"Done Cluster Health, status: {}",
					client.admin()
							.cluster()
							.health(clusterHealthRequest().waitForGreenStatus())
							.get().getStatus());
			logger.info("Number of documents indexed from afterBulk: {}",
					indexedDocumentCount.get());
			client.admin().indices().prepareRefresh(INDEX_NAME).get();
			long count = client.prepareCount(INDEX_NAME).get().getCount();
			logger.info("Number of documents: {} in index {}", count,
					INDEX_NAME);
			if (count != MAX) {
				throw new RuntimeException(
						String.format(
								"Number of documents indexed %s does not match the target %s",
								count, MAX));
			}
		} catch (Throwable t) {
			logger.error("testBulkProcessor failed", t);
		} finally {
			watcher.stop();
			logger.info("Elpased time: {}", watcher.toString());
			if (client.admin().indices().prepareExists(INDEX_NAME).get()
					.isExists()) {
				client.admin().indices().prepareDelete(INDEX_NAME).get();
				try {
					client.admin()
							.cluster()
							.health(clusterHealthRequest().waitForGreenStatus())
							.get().getStatus();
				} catch (Throwable t) {
					throw new RuntimeException(t);
				}
			}
		}
	}

	private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

		public void beforeBulk(long executionId, BulkRequest request) {
		}

		public void afterBulk(long executionId, BulkRequest request,
				Throwable failure) {
		}

		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse response) {
			if (response.hasFailures()) {
				throw new RuntimeException(response.buildFailureMessage());
			}
			indexedDocumentCount.addAndGet(response.getItems().length);
		}
	};
}
