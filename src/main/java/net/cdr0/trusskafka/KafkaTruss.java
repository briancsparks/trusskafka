package net.cdr0.trusskafka;

import net.cdr0.truss.Truss;
import net.cdr0.truss.kind.LogAttr;
import net.cdr0.truss.kind.LogItem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaTruss extends Truss {

  private Producer<String, String> producer = null;

  public KafkaTruss(Truss sourceTruss) {
    super(sourceTruss);

    sourceTruss.setDestination(this);

    System.out.printf("Starting Kafka(%d/2)\n", 1);

    // setup Kafka
    Properties props = new Properties();
//    props.put("bootstrap.servers", "sparksb6-wsl2.cdr0.net:9092");
    props.put("bootstrap.servers", "127.0.0.1:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("linger.ms", 1);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    producer = new KafkaProducer<>(props);
    System.out.printf("Starting Kafka(%d/2)\n", 2);

  }

  public void shutdown() {
    System.out.printf("Stopping Kafka(%d/2)\n", 1);

    producer.close();
    producer = null;

    System.out.printf("Stopping Kafka(%d/2)\n", 2);
  }

  // ------------------------------------------------------------------------------------------------------------------
  @Override
  public void addLogItem(LogItem item) {
//    output.add(item.toString());

    String itemJson = item.toString();
    producer.send(new ProducerRecord<>("rabbittopic", "LogItem", itemJson));
  }

  //  // ------------------------------------------------------------------------------------------------------------------
//  @Override
//  public void addLogItem(LogBase item) {
////    output.add(item.toString());
//
//    String itemJson = item.toString();
//    producer.send(new ProducerRecord<>("rabbittopic", "LogItem", itemJson));
//  }
//
  // ------------------------------------------------------------------------------------------------------------------
  @Override
  public void addLogAttr(LogAttr logAttr) {
  }

}
