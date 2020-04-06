package com.example.seller;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Advertisement extends JsonObject {

  private String id;

  private String name;

  @Override
  public String encode() {
    return String.format("{ \"id\" : \"%s\", \"name\" : \"%s\" }", id, name);
  }

}
