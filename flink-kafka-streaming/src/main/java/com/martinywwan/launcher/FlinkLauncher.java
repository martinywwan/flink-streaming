package com.martinywwan.launcher;

import com.martinywwan.config.ApplicationProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class FlinkLauncher implements ApplicationRunner {

    private final ApplicationProperties applicationProperties;

    @Autowired
    public FlinkLauncher(ApplicationProperties applicationProperties){
        this.applicationProperties = applicationProperties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationProperties.getKafka().getBootStrapServer());
        System.out.println(" getBootStrapServer ID : " + applicationProperties.getKafka().getBootStrapServer());
        System.out.println(" group ID : " + applicationProperties.getKafka().getGroupId());
        System.out.println("offset: "+ applicationProperties.getKafka().getAutoOffsetReset());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, applicationProperties.getKafka().getGroupId());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, applicationProperties.getKafka().getAutoOffsetReset());
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(applicationProperties.getKafka().getTopicName(), new SimpleStringSchema(), properties);
        streamExecutionEnvironment.addSource(consumer011).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                System.out.println("value " + value);
            }
        });
        streamExecutionEnvironment.execute("Flink-Kafka-Streaming Application");
    }
}
