package kafkawordcount

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time



object KeyedStream{

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

    var Count = 0

    val windowCounts = messageStream
	.flatMap { w => w.split("\\s") }
        .map { (_, 1) }
        .setParallelism(3)
        .keyBy(0)
        .reduce { (kC1, kC2) => (kC1, kC2) match { 
         case ((key1, count1), (key2, count2)) =>  
           (key1, count1 + count2) 
       }}


    windowCounts.print()

    env.execute("Read from Kafka and Word Count")
  }

    // Data type for words with count
    case class WordWithCount(word: String, count: Long)
}
