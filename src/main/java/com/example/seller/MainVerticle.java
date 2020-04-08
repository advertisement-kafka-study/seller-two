package com.example.seller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.json.Json;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
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
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class MainVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(MainVerticle.class);

  private final AtomicInteger counter = new AtomicInteger(1);

  private final ObjectMapper objectMapper = new ObjectMapper();

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

        KafkaProducer<String, String> kafkaTemplate = getKafkaProducer(properties);

        vertx.setPeriodic(1000L, value -> {
          Advertisement advertisement = new Advertisement();
          advertisement.setId(String.valueOf(counter.getAndIncrement()));
          advertisement.setName(applicationName);

          CloudEventImpl<Advertisement> cloudEvent = CloudEventBuilder.<Advertisement>builder()
            .withId(UUID.randomUUID().toString())
            .withSource(URI.create("/advertisements/" + advertisement.getId()))
            .withType("Advertisement")
            .withTime(ZonedDateTime.now(ZoneOffset.UTC))
            .withData(advertisement)
            .build();

          KafkaProducerRecord<String, String> record = KafkaProducerRecord
            .create(topic, applicationName, Json.encode(cloudEvent));

          kafkaTemplate.write(record, kafkafCallback -> {
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

  private KafkaProducer<String, String> getKafkaProducer(JsonObject properties) {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", properties.getString("kafka.bootstrap-servers"));
    config.put("key.serializer", properties.getString("kafka.producer.key-serializer"));
    config.put("value.serializer", properties.getString("kafka.producer.value-serializer"));
    config.put("acks", "1");

    return KafkaProducer.create(vertx, config);
  }

}
