## MODIFIED Requirements

### Requirement: Inclusive bounded confidence thresholds

Public confidence options SHALL accept only finite values in the documented range. A match at the configured minimum SHALL satisfy that threshold. Empty samples and invalid limits SHALL not cause division errors. When a scan does not set a minimum confidence, the CLI, SDK, HTTP API and MCP server SHALL use 50.

#### Scenario: Perfect match at the maximum threshold
- **WHEN** all eligible sampled values match a value rule and the minimum confidence is 100 percent
- **THEN** the rule match is retained with confidence 100 percent

#### Scenario: Empty denominator is handled
- **WHEN** no eligible values remain after sample and empty-value policies
- **THEN** classification produces the documented empty/unknown outcome without division by zero

#### Scenario: Weak matches hidden by default
- **WHEN** one of four values in a column is an email address
- **AND** the scan does not set a minimum confidence
- **THEN** the field reports no email match
- **AND** with a minimum confidence of 5 the field reports the email match at 25
