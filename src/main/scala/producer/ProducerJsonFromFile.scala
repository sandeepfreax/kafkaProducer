package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger

import scala.io.Source


object ProducerJsonFromFile extends App {

  lazy val logger = Logger.getLogger(this.getClass)

  //Topic to which we are going to write messages
  val topicName = "record.dx.eventAllOne"

  //Configurations for Kadka Producer
  case class KafkaProducerConfigs(brokerList: String = "127.0.0.1:9092") {
    val properties = new Properties()
    properties.put("bootstrap.servers", brokerList)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])
  }

  //Create instance of producer with all relevant configurations
  val producer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)

  logger.info("Starting the Producer to write to - " + topicName)

  try{
    //Read stream from Resources folder
    val getStream = getClass.getResourceAsStream("/31082019.json")
    def getContentLines = Source.fromInputStream(getStream).getLines().toList
    //get each line one by one
    for (line <- getContentLines) {
      //producer sends record
      producer.send(new ProducerRecord[String, String](topicName, line))
      //explicitly wait for this interval before writing another message
      Thread.sleep(500)
    }
  } catch {
    case ex: Throwable => {
      logger.error("Caught in exception while writing content to Kafka Topic : " + topicName)
      logger.error("Exception encountered: " + ex.printStackTrace())
    }
  } finally {
    //close the producer
    producer.close()
    logger.info("Producer closed")
  }
}
