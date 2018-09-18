package com.oppo.dc.ostream;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@EnableJpaRepositories("com.oppo.dc.ostream.repository")
@Configuration
@SpringBootApplication
@ComponentScan("com.oppo.dc.ostream")
public class OSteramTableConfig {
}
