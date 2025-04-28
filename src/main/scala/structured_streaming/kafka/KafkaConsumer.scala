package structured_streaming.kafka

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val topic = "my-topic" // Kafka主题
    val brokers = "127.0.0.1:9093" // Kafka集群地址
    val groupId = "test_group" // 消费者组

    // 配置Kafka消费者
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupId)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest") // earliest从最早的偏移量开始消费

    val consumer = new KafkaConsumer[String, String](props)

    // 订阅主题
    consumer.subscribe(Collections.singletonList(topic))

    // 开始消费消息
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(1000) // 拉取消息
        for (record <- records.asScala) {
          println(s"收到消息: key = ${record.key()}, value = ${record.value()}, offset = ${record.offset()}, partition = ${record.partition()}")
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}
