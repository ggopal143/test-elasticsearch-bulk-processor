package com.github.richardwilly98;

import static org.elasticsearch.client.Requests.indexRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
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
    public final static TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueMillis(10);
    public final static ByteSizeValue DEFAULT_BULK_SIZE = new ByteSizeValue(5, ByteSizeUnit.MB);

    private final BulkProcessor bulkProcessor;
	private final TransportClient client;
	private static final String INDEX_NAME = "dummy-"
			+ System.currentTimeMillis();
	private static final String TYPE_NAME = "my-type";
	private static final long MAX = 1000000;

	private long indexedDocumentCount = 0;
	public TestBulkProcessor(TransportClient client) {
		this.client = client;
		this.bulkProcessor = BulkProcessor.builder(client, listener)
				.setBulkActions(DEFAULT_BULK_ACTIONS)
				.setConcurrentRequests(8)
//				.setFlushInterval(DEFAULT_FLUSH_INTERVAL)
				.setBulkSize(DEFAULT_BULK_SIZE)
				.build();
	}

	public static void main(String[] args) {
		TransportClient client = new TransportClient();
		client.addTransportAddress(new InetSocketTransportAddress("localhost",
				9300));
		TestBulkProcessor test = new TestBulkProcessor(client);
		test.testBulkProcessor();
		client.close();
	}

	void testBulkProcessor() {
		try {
			if (client.admin().indices().prepareExists(INDEX_NAME).get()
					.isExists()) {
				client.admin().indices().prepareDelete(INDEX_NAME).get();
			}
			client.admin().indices().prepareCreate(INDEX_NAME).get();
			for (int i = 0; i < MAX; i++) {
				Map<String, Object> data = new HashMap<String, Object>();
				data.put("name", "test-" + i);
				data.put("date", new Date());
				bulkProcessor.add(indexRequest(INDEX_NAME).type(TYPE_NAME)
						.source(XContentFactory.jsonBuilder().map(data)));
			}
			bulkProcessor.close();
			Thread.sleep(10000);
			logger.info("Number of documents indexed from afterBulk: {}", indexedDocumentCount);
			client.admin().indices().prepareRefresh(INDEX_NAME).get();
			long count = client.prepareCount(INDEX_NAME).get().getCount();
			logger.info("Number of documents: {} in index {}", count,
					INDEX_NAME);
			if (count != MAX) {
				throw new RuntimeException(String.format("Number of documents indexed %s does not match the target %s", count, MAX));
			}
		} catch (Throwable t) {
			logger.error("testBulkProcessor failed", t);
		}
	}

	private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

		public void beforeBulk(long executionId, BulkRequest request) {
		}

		public void afterBulk(long executionId, BulkRequest request,
				Throwable failure) {
			throw new RuntimeException(failure);
		}

		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse response) {
			indexedDocumentCount += response.getItems().length;

		}
	};
}
