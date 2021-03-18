package com.consumer.config;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.consumer.AwsSdkConsumerApplication;

public class AmazonKinesisGet {

	private static final Logger log = LoggerFactory.getLogger(AmazonKinesisSample.class);

	@Autowired
	private static AmazonKinesis kinesisClient;

	AwsKinesisClient kinesis = new AwsKinesisClient();

	public static void init() throws Exception {

	}

	public void getKinesisShard() {
		kinesisClient = kinesis.getKinesisClient();

		final String myStreamName = "Poc-Test";
		final Integer myStreamSize = 1;

		// list all of my streams
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(10);
		ListStreamsResult listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		log.info("Streams is", streamNames);
		while (listStreamsResult.isHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
			}

			listStreamsResult = kinesisClient.listStreams(listStreamsRequest);

			streamNames.addAll(listStreamsResult.getStreamNames());

		}
		log.info("Printing my list of streams : ");

		// print all of my streams.
		if (!streamNames.isEmpty()) {
			System.out.println("List of my streams: ");
		}
		for (int i = 0; i < streamNames.size(); i++) {
			System.out.println(streamNames.get(i));
		}

		// System.out.println(streamNames.get(0));
		String myownstream = streamNames.get(0);

		// Retrieve the Shards from a Stream
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(myownstream);
		DescribeStreamResult describeStreamResult;
		List<Shard> shards = new ArrayList<>();
		String lastShardId = null;

		do {
			describeStreamRequest.setExclusiveStartShardId(lastShardId);
			describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
			shards.addAll(describeStreamResult.getStreamDescription().getShards());
			if (shards.size() > 0) {
				lastShardId = shards.get(shards.size() - 1).getShardId();
			}
		} while (describeStreamResult.getStreamDescription().getHasMoreShards());

		// Get Data from the Shards in a Stream
		// Hard-coded to use only 1 shard
		String shardIterator;
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(myownstream);
		// get(0) shows hardcoded to 1 stream
		getShardIteratorRequest.setShardId(shards.get(0).getShardId());
		// using TRIM_HORIZON but could use alternatives
		getShardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST);

		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();

		// Continuously read data records from shard.
		List<com.amazonaws.services.kinesis.model.Record> records;

		while (true) {
			// Create new GetRecordsRequest with existing shardIterator.
			// Set maximum records to return to 1000.

			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setShardIterator(shardIterator);
			getRecordsRequest.setLimit(1000);

			GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);

			// Put result into record list. Result may be empty.
			records = result.getRecords();

			// Print records
			for (com.amazonaws.services.kinesis.model.Record record : records) {
				ByteBuffer byteBuffer = record.getData();
				System.out.println(byteBuffer.toString());
				System.out.println(
						String.format("Seq No: %s - %s", record.getSequenceNumber(), new String(byteBuffer.array())));
				waitForStreamToBecomeAvailable(myStreamName);
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				throw new RuntimeException(exception);
			}

			shardIterator = result.getNextShardIterator();
		}

	}
	
	 private static void waitForStreamToBecomeAvailable(String myStreamName) {

	        System.out.println("Waiting for " + myStreamName + " to become ACTIVE...");

	        long startTime = System.currentTimeMillis();
	        long endTime = startTime + (10 * 60 * 1000);
	        while (System.currentTimeMillis() < endTime) {
	            try {
	                Thread.sleep(1000 * 20);
	            } catch (InterruptedException e) {
	                // Ignore interruption (doesn't impact stream creation)
	            }
	            try {
	                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
	                describeStreamRequest.setStreamName(myStreamName);
	                // ask for no more than 10 shards at a time -- this is an optional parameter
	               // describeStreamRequest.setLimit(10);
	                DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

	                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
	                System.out.println("  - current state: " + streamStatus);
	                if (streamStatus.equals("ACTIVE")) {
	                    return;
	                }
	            } catch (AmazonServiceException ase) {
	                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) {
	                    throw ase;
	                }
	                throw new RuntimeException("Stream " + myStreamName + " never went active");
	            }
	        }
	    }

}
