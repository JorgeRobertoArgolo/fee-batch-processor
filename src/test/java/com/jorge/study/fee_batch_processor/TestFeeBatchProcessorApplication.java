package com.jorge.study.fee_batch_processor;

import org.springframework.boot.SpringApplication;

public class TestFeeBatchProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.from(FeeBatchProcessorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
