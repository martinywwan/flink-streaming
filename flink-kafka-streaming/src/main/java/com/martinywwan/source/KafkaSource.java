package com.martinywwan.source;

import com.martinywwan.config.ApplicationProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.Properties;

@Configuration
public class KafkaSource {

    private final ApplicationProperties applicationProperties;

    @Autowired
    public KafkaSource(ApplicationProperties applicationProperties){
        this.applicationProperties = applicationProperties;
    }

    @Bean
    @Lazy
    public FlinkKafkaConsumer011<String> kafkaConsumer(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationProperties.getKafka().getBootStrapServer());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, applicationProperties.getKafka().getGroupId());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, applicationProperties.getKafka().getAutoOffsetReset());
        return new FlinkKafkaConsumer011<>(applicationProperties.getKafka().getTopicName(), new SimpleStringSchema(), properties);
    }
}
