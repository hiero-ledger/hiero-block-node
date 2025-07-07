# Publisher Plugin Test Plan

## Overview

<!-- Provide a brief overview of the feature or component under test. Provide a link to the design document or HIP if available. -->
The Goal of this test plan is to ensure that the Plublisher plugin works as expected, in and E2E dockerized environment, support for real expected use cases and load, with the ability to increase load and frequency of requests, and to ensure that the plugin is stable and reliable, but also able to handle unexpected situations gracefully, such as network failures, server crashes, and other issues that may arise during normal operation.


## Key Considerations

<!-- Provide a list of more intricate considerations that are note-worthy in the implementation or experience. -->

There are 3 main set-ups that we need to consider when testing the Publisher plugin:
1. **Single Publisher per BN**: These scenarios is the simplest one, where we have a single publisher for each BN, however this setup is realistic to be used on some networks, including even some public ones.
2. **Multiple Publishers per BN**: These scenarios are more complex, where we have multiple publishers for each BN, this is a more realistic setup for most networks, and involves more complex interactions between the publishers and the BNs.
3. **Multiple Publishers with Multiple BNs**: These scenarios are the most complex, where we have multiple publishers and multiple BNs, this is a realistic setup for most networks, and involves the most complex interactions between the publishers and the BNs.

Hence, we will divide the test cases into 3 groups, each group will cover one of the setups above, and will include the most common scenarios that are expected to be used in the real world, as well as some edge cases and exceptional cases that may arise during normal operation.

## Test Scenarios

|              Test Case ID | Test Name                                                            | Implemented ( :white_check_mark: / :x: ) |
|--------------------------:|:---------------------------------------------------------------------|:----------------------------------------:|
|                           | _**<br/>[Single Publisher to Single BN](#test-group-1)<br/>&nbsp;**_ |                                          |
| [TC-TG1-001](#tc-tg1-001) | Basic Functionality Test 1 Group 1                                   |                   :x:                    |
| [TC-TG1-002](#tc-tg1-002) | Basic Functionality Test 1 Group 1                                   |            :white_check_mark:            |
|                           | _**<br/>[Test Group 2](#test-group-2)<br/>&nbsp;**_                  |                                          |
| [TC-TG2-001](#tc-tg2-001) | Basic Functionality Test 1 Group 2                                   |                   :x:                    |

### Single Publisher to Single BN

#### TC-TG1-001

##### Test Name

`Basic Functionality Test 1 Group 1`

##### Scenario Description

<!-- Describe the scenario of the test. -->

##### Requirements

<!-- What are the requirement(s) that the test should aim to verify. -->

##### Expected Behaviour

<!-- What is the expected behavior/outcome that would make the test successful. -->

##### Preconditions

<!-- What are the preconditions that will allow us to run the test? -->

##### Input

<!-- What is the input of the test? -->

##### Output

<!-- What output/result is expected to be produced at the end of the test? -->

##### Other

<!-- Additional information, if applicable -->

---

#### TC-TG1-002

##### Test Name

`Basic Functionality Test 1 Group 2`

##### Scenario Description

<!-- Describe the scenario of the test. -->

##### Requirements

<!-- What are the requirement(s) that the test should aim to verify. -->

##### Expected Behaviour

<!-- What is the expected behavior/outcome that would make the test successful. -->

##### Preconditions

<!-- What are the preconditions that will allow us to run the test? -->

##### Input

<!-- What is the input of the test? -->

##### Output

<!-- What output/result is expected to be produced at the end of the test? -->

##### Other

<!-- Additional information, if applicable -->

---

### Test Group 2

#### TC-TG2-001

##### Test Name

`Basic Functionality Test 1 Group 2`

##### Scenario Description

<!-- Describe the scenario of the test. -->

##### Requirements

<!-- What are the requirement(s) that the test should aim to verify. -->

##### Expected Behaviour

<!-- What is the expected behavior/outcome that would make the test successful. -->

##### Preconditions

<!-- What are the preconditions that will allow us to run the test? -->

##### Input

<!-- What is the input of the test? -->

##### Output

<!-- What output/result is expected to be produced at the end of the test? -->

##### Other

<!-- Additional information, if applicable-->

---
