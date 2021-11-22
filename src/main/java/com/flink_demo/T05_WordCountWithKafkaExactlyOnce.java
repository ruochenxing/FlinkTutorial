package com.flink_demo;

import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 这里的报错会导致flink task一直重启，因为Kafka中的null字符串会一直消费，不像nc -l 7777那样消费一次就没了
 * 
 */
public class T05_WordCountWithKafkaExactlyOnce {
	public static void main(String[] args) throws Exception {

		/*
		 * ParameterTool parameters = ParameterTool.fromArgs(args);
		 */
		// 上述方式可以将传入的配置参数读取，但是在实际生产环境最佳放在指定的文件中
		// 而不是放在maven 项目的resources中，因为打完包后，想要修改就不太方便了
		// Flink 官方最佳实践
		// /Users/ruochenxing/Desktop/projects/FlinkTutorial/src/main/java/com/flink_demo/T05_config.properties
		ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
		// # 创建topic
		// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1
		// --partitions 1 --topic activity10
		//
		// # 创建生产者
		// kafka-console-producer.sh --broker-list localhost:9092 --topic activity10

		DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines
				.map(new MapFunction<String, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 2280246421094034977L;

					@Override
					public Tuple2<String, Integer> map(String word) throws Exception {
						if (word.startsWith("null")) {
							throw new RuntimeException("输入为null，发生异常");
						}
						return Tuple2.of(word, 1);
					}
				});
		SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);
		summed.print();
		FlinkUtils.getEnv().execute("T04_WordCountPro");
	}
}
