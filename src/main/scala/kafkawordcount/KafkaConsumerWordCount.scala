package kafkawordcount

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time


object KafkaConsumerWordCount{

  def main(args: Array[String]): Unit = {

    // parse input arguments
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\nUsage: Kafka --topic <topic> " +
        "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>")
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    	//env.getConfig.disableSysoutLogging
    	//env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    	//env.enableCheckpointing(5000)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer = new FlinkKafkaConsumer010(
      params.getRequired("topic"),
      new SimpleStringSchema,
      params.getProperties)
    val messageStream = env.addSource(kafkaConsumer)

    val windowCounts = messageStream
	.flatMap { w => w.split("\\s") }
        .map { w => WordWithCount(w, 1) }
        .keyBy("word")
        .timeWindow(Time.seconds(1))
        .sum("count")

    windowCounts.print()

    env.execute("Read from Kafka and Word Count")
  }

    // Data type for words with count
    case class WordWithCount(word: String, count: Long)
}
