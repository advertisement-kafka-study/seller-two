package com.example.seller;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MainVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(MainVerticle.class);

  private final AtomicInteger counter = new AtomicInteger(1);

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(MainVerticle.class.getName());
  }

  @Override
  public void start(Promise<Void> startPromise) {
    ConfigStoreOptions configStoreOptions = getConfigStoreOptions();

    ConfigRetrieverOptions configRetrieverOptions = new ConfigRetrieverOptions()
      .addStore(configStoreOptions);

    ConfigRetriever.create(vertx, configRetrieverOptions).getConfig(callback -> {
      if (callback.succeeded()) {
        JsonObject properties = callback.result();
        String applicationName = properties.getString("application.name");
        String topic = properties.getString("kafka.template.default-topic");

        KafkaProducer<String, Advertisement> kafkaTemplate = getKafkaProducer(properties);

        vertx.setPeriodic(1000L, value -> {
          Advertisement advertisement = new Advertisement();
          advertisement.setId(String.valueOf(counter.getAndIncrement()));
          advertisement.setName(applicationName);

          kafkaTemplate.send(KafkaProducerRecord.create(topic, applicationName, advertisement),
            kafkafCallback -> {
              if (kafkafCallback.succeeded()) {
                log.info(
                  String.format("Published Message=[%s] to Topic=[%s]", advertisement, topic));
              } else {
                log.error("Error to publish message", kafkafCallback.cause());
              }
            });
        });
      } else {
        log.error("Error to retrieve configuration", callback.cause());
      }
    });
  }

  private ConfigStoreOptions getConfigStoreOptions() {
    return new ConfigStoreOptions()
      .setType("file")
      .setFormat("properties")
      .setConfig(new JsonObject().put("path", "application.properties"));
  }

  private KafkaProducer<String, Advertisement> getKafkaProducer(JsonObject properties) {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", properties.getString("kafka.bootstrap-servers"));
    config.put("key.serializer", properties.getString("kafka.producer.key-serializer"));
    config.put("value.serializer", properties.getString("kafka.producer.value-serializer"));
    config.put("acks", "1");

    return KafkaProducer.create(vertx, config);
  }

}
