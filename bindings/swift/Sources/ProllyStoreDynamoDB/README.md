# Swift DynamoDB store

`ProllyStoreDynamoDB` borrows a Soto `DynamoDB` service client. It uses one
binary `pk`, a binary `value`, strongly consistent reads, conditional CAS, and
native transactions. Closing the adapter does not shut down the borrowed
`AWSClient`.
