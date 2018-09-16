package com.oppo.dc;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@EnableJpaRepositories("**.repository")
@Configuration
@SpringBootApplication
@ComponentScan
public class OSteramTableConfig {
}
