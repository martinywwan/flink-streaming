package com.martinywwan.launcher;

import com.martinywwan.config.ApplicationProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
    private FlinkKafkaConsumer011<String> kafkaConsumer;

    @Autowired
    public FlinkLauncher(ApplicationProperties applicationProperties){
        this.applicationProperties = applicationProperties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaDataStreamSource = streamExecutionEnvironment.addSource(kafkaConsumer); //add a data source
        kafkaDataStreamSource.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                System.out.println("value " + value);
            }
        });
        streamExecutionEnvironment.execute("Flink Streaming Application");
    }
}
