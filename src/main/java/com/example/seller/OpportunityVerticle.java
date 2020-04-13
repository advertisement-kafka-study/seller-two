package com.example.seller;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.HashMap;
import java.util.Map;

public class OpportunityVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(OpportunityVerticle.class);

  private final JsonObject properties;

  public OpportunityVerticle(JsonObject properties) {
    this.properties = properties;
  }

  @Override
  public void start() throws Exception {
    KafkaConsumer<String, String> consumer = createKafkaConsumer();
    consumer.subscribe(properties.getString("opportunity-topic"), asyncResult -> {
      if (asyncResult.succeeded()) {
        log.info("Subscribe successfully");
      } else {
        log.error("Fail to subscribe", asyncResult.cause());
      }
    });
    consumer.handler(event -> {
      Opportunity opportunity = Json.decodeValue(event.value(), Opportunity.class);
      log.info("Consuming Message=[{}] Key=[{}]", opportunity, event.key());
      vertx.eventBus().publish(AdvertisementVerticle.EVENT_BUS, Json.encode(opportunity));
    });
  }

  private KafkaConsumer<String, String> createKafkaConsumer() {
    Map<String, String> configurations = new HashMap<>();
    configurations.put("bootstrap.servers", properties.getString("kafka.bootstrap-servers"));
    configurations.put("key.deserializer", properties.getString("kafka.consumer.key-serializer"));
    configurations
      .put("value.deserializer", properties.getString("kafka.consumer.value-serializer"));
    configurations.put("group.id", properties.getString("application.name"));
    configurations.put("auto.offset.reset", "latest");
    configurations.put("enable.auto.commit", "false");

    return KafkaConsumer.create(vertx, configurations);
  }

}
