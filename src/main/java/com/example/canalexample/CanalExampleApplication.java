package com.example.canalexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.example.canalexample.service")
public class CanalExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(CanalExampleApplication.class, args);
	}

}
