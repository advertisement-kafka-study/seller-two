package com.example.seller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OpportunityRequest {

  private String category;

  private OpportunityRequirements requirements;

  private OpportunityCallback callbackData;

}
