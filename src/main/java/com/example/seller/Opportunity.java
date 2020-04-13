package com.example.seller;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Opportunity {

  private String id;

  private String customerKey;

  @JsonProperty("advReq")
  private OpportunityRequest opportunityRequest;

}

