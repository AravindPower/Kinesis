package com.consumer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.consumer.config.AmazonKinesisGet;

@RestController
@RequestMapping(value = "/api")
public class ShardController {

	@GetMapping("/shard")
	public ResponseEntity<String> getShard() {
		AmazonKinesisGet kinesis = new AmazonKinesisGet();
		kinesis.getKinesisShard();
		return ResponseEntity.ok("Data is Fetched");
	}

}
