package structured_streaming.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic = "test-topic" // Kafka主题
    val brokers = "127.0.0.1:9092" // Kafka集群地址

    // 配置Kafka生产者
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // 生成随机单词的列表
    val randomWords = List("apple", "banana", "orange", "grape", "kiwi", "pear", "mango")

    // 每隔1秒发送一个包含多个随机单词的随机字符串到Kafka主题
    while (true) {
      val randomWordCount = Random.nextInt(5) + 1 // 生成1到5个随机单词
      val randomString = (1 to randomWordCount)
        .map(_ => randomWords(Random.nextInt(randomWords.length)))
        .mkString(" ")

      val record = new ProducerRecord[String, String](topic, randomString)
      producer.send(record)
      println("数据发送完毕：" + randomString)
      Thread.sleep(1000)
    }

    producer.close()
  }
}
