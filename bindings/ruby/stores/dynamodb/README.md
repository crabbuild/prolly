# Ruby DynamoDB store

This adapter borrows an official `Aws::DynamoDB::Client`, uses one binary hash
key named `pk` plus a binary `value`, strongly consistent reads, conditional
root CAS, and native DynamoDB transactions. Closing it leaves the client open.
