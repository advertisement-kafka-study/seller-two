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

public class MainVerticle extends AbstractVerticle {

  private final Logger log = LoggerFactory.getLogger(MainVerticle.class);

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
        vertx.deployVerticle(new AdvertisementVerticle(properties));
        vertx.deployVerticle(new OpportunityVerticle(properties));
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

}
