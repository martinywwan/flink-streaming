package com.martinywwan.publisher;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class KafkaPublisher implements ApplicationRunner {

    @Autowired
    private FlinkKafkaProducer011<String> kafkaProducer;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LocalStreamEnvironment localStreamEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> dataStringSource = localStreamEnvironment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                Arrays.asList("hello" , "example").stream().forEach(s -> sourceContext.collect(s));
            }

            @Override
            public void cancel() { }
        });
        dataStringSource.addSink(kafkaProducer);
        localStreamEnvironment.execute("KafkaProducer");
    }

    @Bean
    public FlinkKafkaProducer011<String> kafkaProducer(@Value("${kafka.topic-name}")String kafkaTopicName,
                                                       @Value("${kafka.boot-strap-server}") String kafkaBootStrapServer){
        return new FlinkKafkaProducer011<>(
                kafkaBootStrapServer,            // broker list
                kafkaTopicName,                  // target topic
                new SimpleStringSchema());   // serialization schema
    }
}
