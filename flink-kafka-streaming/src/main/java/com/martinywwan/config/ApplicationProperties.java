package com.martinywwan.config;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@ConfigurationProperties
@Getter
@Setter
@Builder
public class ApplicationProperties {

    public final Kafka kafka = new Kafka();

    @Getter
    @Setter
    public class Kafka {
        private String bootStrapServer;

        private String topicName;

        private String groupId;

        private String autoOffsetReset;
    }
}
