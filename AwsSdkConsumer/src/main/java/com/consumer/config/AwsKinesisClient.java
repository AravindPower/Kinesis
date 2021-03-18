package com.consumer.config;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {

	public static final String AWS_ACCESS_KEY = "aws.accessKeyId";
	public static final String AWS_SECRET_KEY = "aws.secretKey";

	static {
		System.setProperty(AWS_ACCESS_KEY, "accessKey");
		System.setProperty(AWS_SECRET_KEY, "secretkey");
	}

	public AmazonKinesis getKinesisClient() {
		return AmazonKinesisClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
	}

}
