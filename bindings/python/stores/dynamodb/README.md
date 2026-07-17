# Python DynamoDB store

This package adapts a caller-owned low-level `aioboto3` DynamoDB client to the
shared asynchronous store protocol. It uses one binary hash key named `pk` and
a binary `value`, PAY_PER_REQUEST table creation, strongly consistent reads,
conditional root CAS, and native DynamoDB transactions. Closing the adapter
does not close the injected client.
