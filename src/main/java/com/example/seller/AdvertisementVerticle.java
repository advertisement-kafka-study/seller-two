package com.example.seller;

import io.cloudevents.json.Json;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
import io.vertx.core.AbstractVerticle;
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

public class AdvertisementVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(OpportunityVerticle.class);

  public static final String EVENT_BUS = "advertisement";


  private final JsonObject properties;

  public AdvertisementVerticle(JsonObject properties) {
    this.properties = properties;
  }

  @Override
  public void start() throws Exception {
    String applicationName = properties.getString("application.name");
    String topic = properties.getString("kafka.template.default-topic");
    KafkaProducer<String, String> kafkaTemplate = getKafkaProducer();

    vertx.eventBus().consumer(EVENT_BUS, event -> {
      Opportunity opportunity = Json.decodeValue(event.body().toString(), Opportunity.class);

      Advertisement advertisement = new Advertisement();
      advertisement.setId(opportunity.getId());
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
  }

  private KafkaProducer<String, String> getKafkaProducer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", properties.getString("kafka.bootstrap-servers"));
    config.put("key.serializer", properties.getString("kafka.producer.key-serializer"));
    config.put("value.serializer", properties.getString("kafka.producer.value-serializer"));
    config.put("acks", "1");

    return KafkaProducer.create(vertx, config);
  }

}
