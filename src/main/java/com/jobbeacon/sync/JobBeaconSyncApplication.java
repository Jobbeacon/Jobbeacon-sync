package com.jobbeacon.sync;

import com.jobbeacon.sync.config.SyncProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SyncProperties.class)
public class JobBeaconSyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobBeaconSyncApplication.class, args);
    }
}
