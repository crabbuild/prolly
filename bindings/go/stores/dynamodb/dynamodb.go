package dynamodb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	prolly "build.crab/prolly-go"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const (
	batchGetLimit    = 100
	batchWriteLimit  = 25
	transactionLimit = 100
	batchRetryLimit  = 8
	partitionKeyAttr = "pk"
	valueAttr        = "value"
)

var (
	nodeFamily = []byte("node:")
	rootFamily = []byte("root:")
	hintFamily = []byte("hint:")
)

// Client is the subset of the AWS SDK v2 DynamoDB client used by Store.
type Client interface {
	BatchGetItem(context.Context, *awsdynamodb.BatchGetItemInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error)
	BatchWriteItem(context.Context, *awsdynamodb.BatchWriteItemInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error)
	CreateTable(context.Context, *awsdynamodb.CreateTableInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.CreateTableOutput, error)
	DeleteItem(context.Context, *awsdynamodb.DeleteItemInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error)
	DeleteTable(context.Context, *awsdynamodb.DeleteTableInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteTableOutput, error)
	DescribeTable(context.Context, *awsdynamodb.DescribeTableInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.DescribeTableOutput, error)
	GetItem(context.Context, *awsdynamodb.GetItemInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error)
	PutItem(context.Context, *awsdynamodb.PutItemInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error)
	Scan(context.Context, *awsdynamodb.ScanInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error)
	TransactWriteItems(context.Context, *awsdynamodb.TransactWriteItemsInput, ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error)
}

type Options struct {
	TableName       string
	AdapterName     string
	KeyPrefix       []byte
	ReadParallelism uint32
}

type Store struct {
	client  Client
	options Options
}

func New(client Client, options Options) *Store {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "dynamodb-v1"
	}
	if options.KeyPrefix == nil {
		options.KeyPrefix = []byte("prolly:")
	}
	options.KeyPrefix = clone(options.KeyPrefix)
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 16
	}
	return &Store{client: client, options: options}
}

func (s *Store) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	readLimit, writeLimit, txLimit := uint32(batchGetLimit), uint32(batchWriteLimit), uint32(transactionLimit)
	return prolly.StoreDescriptor{
		ProtocolMajor: prolly.StoreProtocolMajor, AdapterName: s.options.AdapterName, Provider: "dynamodb", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: true, AtomicBatchWrites: false, NodeScan: true, Hints: true,
			AtomicNodesAndHint: false, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
		Limits: prolly.StoreLimits{
			MaxBatchReadItems: &readLimit, MaxBatchWriteItems: &writeLimit,
			MaxTransactionOperations: &txLimit,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.nodeKey(key), "get_node")
}

func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	return s.put(ctx, s.nodeKey(key), value, "put_node")
}

func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	return s.delete(ctx, s.nodeKey(key), "delete_node")
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	requests := make([]types.WriteRequest, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Value.Present {
			requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: s.item(s.nodeKey(mutation.Key), mutation.Value.Value)}})
		} else {
			requests = append(requests, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: s.keyItem(s.nodeKey(mutation.Key))}})
		}
	}
	return s.batchWrite(ctx, requests)
}

func (s *Store) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	values := make(map[string]prolly.OptionalBytes, len(keys))
	unique := make([][]byte, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		storageKey := s.nodeKey(key)
		encoded := string(storageKey)
		if _, ok := seen[encoded]; !ok {
			seen[encoded] = struct{}{}
			unique = append(unique, storageKey)
		}
	}
	for start := 0; start < len(unique); start += batchGetLimit {
		end := min(start+batchGetLimit, len(unique))
		pending := make([]map[string]types.AttributeValue, 0, end-start)
		for _, key := range unique[start:end] {
			pending = append(pending, s.keyItem(key))
		}
		for attempt := 0; len(pending) != 0; attempt++ {
			output, err := s.client.BatchGetItem(ctx, &awsdynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
				s.options.TableName: {Keys: pending, ConsistentRead: boolPointer(true), ProjectionExpression: stringPointer("#pk, #value"), ExpressionAttributeNames: map[string]string{"#pk": partitionKeyAttr, "#value": valueAttr}},
			}})
			if err != nil {
				return nil, dynamoError("batch_get", err)
			}
			for _, item := range output.Responses[s.options.TableName] {
				key, keyErr := binaryAttribute(item, partitionKeyAttr)
				value, valueErr := binaryAttribute(item, valueAttr)
				if keyErr != nil {
					return nil, keyErr
				}
				if valueErr != nil {
					return nil, valueErr
				}
				values[string(key)] = prolly.PresentBytes(value)
			}
			pending = nil
			if unprocessed, ok := output.UnprocessedKeys[s.options.TableName]; ok {
				pending = unprocessed.Keys
			}
			if len(pending) != 0 {
				if attempt+1 >= batchRetryLimit {
					return nil, limitError("DynamoDB batch get left %d keys unprocessed", len(pending))
				}
				if err := waitBackoff(ctx, attempt); err != nil {
					return nil, err
				}
			}
		}
	}
	result := make([]prolly.OptionalBytes, len(keys))
	for index, key := range keys {
		if value, ok := values[string(s.nodeKey(key))]; ok {
			result[index] = value
		}
	}
	return result, nil
}

func (s *Store) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	keys, err := s.scanKeys(ctx, s.familyPrefix(nodeFamily))
	if err != nil {
		return nil, err
	}
	prefix := s.familyPrefix(nodeFamily)
	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if suffix := bytes.TrimPrefix(key, prefix); len(suffix) == 32 && len(key) == len(prefix)+32 {
			result = append(result, clone(suffix))
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result, nil
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.hintKey(namespace, key), "get_hint")
}

func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	return s.put(ctx, s.hintKey(namespace, key), value, "put_hint")
}

func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	mutations := make([]prolly.NodeMutation, len(nodes))
	for index, node := range nodes {
		mutations[index] = prolly.UpsertNode(node.Key, node.Value)
	}
	if err := s.BatchNodes(ctx, mutations); err != nil {
		return err
	}
	return s.PutHint(ctx, namespace, key, value)
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.rootKey(name), "get_root")
}

func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	return s.put(ctx, s.rootKey(name), manifest, "put_root")
}

func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	return s.delete(ctx, s.rootKey(name), "delete_root")
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	key := s.rootKey(name)
	condition, names, values := rootCondition(expected)
	var err error
	if replacement.Present {
		_, err = s.client.PutItem(ctx, &awsdynamodb.PutItemInput{
			TableName: &s.options.TableName, Item: s.item(key, replacement.Value),
			ConditionExpression: &condition, ExpressionAttributeNames: names,
			ExpressionAttributeValues: values, ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		})
	} else {
		_, err = s.client.DeleteItem(ctx, &awsdynamodb.DeleteItemInput{
			TableName: &s.options.TableName, Key: s.keyItem(key),
			ConditionExpression: &condition, ExpressionAttributeNames: names,
			ExpressionAttributeValues: values, ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		})
	}
	if err == nil {
		return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
	}
	var conditional *types.ConditionalCheckFailedException
	if !errors.As(err, &conditional) {
		return prolly.RootCASResult{}, dynamoError("root_cas", err)
	}
	current, getErr := s.get(ctx, key, "root_cas_read")
	return prolly.RootCASResult{Current: current}, getErr
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	prefix := s.familyPrefix(rootFamily)
	keys, err := s.scanKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}
	names := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if bytes.HasPrefix(key, prefix) {
			names = append(names, clone(key[len(prefix):]))
		}
	}
	sort.Slice(names, func(i, j int) bool { return bytes.Compare(names[i], names[j]) < 0 })
	result := make([]prolly.NamedStoreRoot, 0, len(names))
	for _, name := range names {
		manifest, err := s.GetRootManifest(ctx, name)
		if err != nil {
			return nil, err
		}
		if manifest.Present {
			result = append(result, prolly.NamedStoreRoot{Name: name, Manifest: manifest.Value})
		}
	}
	return result, nil
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	rootWrites := make(map[string]prolly.RootWrite, len(roots))
	for _, root := range roots {
		rootWrites[string(root.Name)] = root
	}
	count := len(nodes) + len(roots)
	for _, condition := range conditions {
		if _, written := rootWrites[string(condition.Name)]; !written {
			count++
		}
	}
	if count > transactionLimit {
		return prolly.StoreTransactionResult{}, limitError("DynamoDB transaction has %d operations, exceeding the %d operation limit", count, transactionLimit)
	}
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	conditionByName := make(map[string]prolly.RootCondition, len(conditions))
	items := make([]types.TransactWriteItem, 0, count)
	for _, condition := range conditions {
		conditionByName[string(condition.Name)] = condition
		if _, written := rootWrites[string(condition.Name)]; written {
			continue
		}
		expression, names, values := rootCondition(condition.Expected)
		items = append(items, types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName: &s.options.TableName, Key: s.keyItem(s.rootKey(condition.Name)),
			ConditionExpression: &expression, ExpressionAttributeNames: names, ExpressionAttributeValues: values,
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}})
	}
	for _, root := range roots {
		condition, hasCondition := conditionByName[string(root.Name)]
		if root.Replacement.Present {
			put := &types.Put{TableName: &s.options.TableName, Item: s.item(s.rootKey(root.Name), root.Replacement.Value)}
			if hasCondition {
				expression, names, values := rootCondition(condition.Expected)
				put.ConditionExpression, put.ExpressionAttributeNames, put.ExpressionAttributeValues = &expression, names, values
				put.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
			}
			items = append(items, types.TransactWriteItem{Put: put})
		} else {
			deletion := &types.Delete{TableName: &s.options.TableName, Key: s.keyItem(s.rootKey(root.Name))}
			if hasCondition {
				expression, names, values := rootCondition(condition.Expected)
				deletion.ConditionExpression, deletion.ExpressionAttributeNames, deletion.ExpressionAttributeValues = &expression, names, values
				deletion.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
			}
			items = append(items, types.TransactWriteItem{Delete: deletion})
		}
	}
	for _, node := range nodes {
		if node.Value.Present {
			items = append(items, types.TransactWriteItem{Put: &types.Put{TableName: &s.options.TableName, Item: s.item(s.nodeKey(node.Key), node.Value.Value)}})
		} else {
			items = append(items, types.TransactWriteItem{Delete: &types.Delete{TableName: &s.options.TableName, Key: s.keyItem(s.nodeKey(node.Key))}})
		}
	}
	if len(items) == 0 {
		return prolly.StoreTransactionResult{Applied: true}, nil
	}
	_, err := s.client.TransactWriteItems(ctx, &awsdynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return prolly.StoreTransactionResult{Applied: true}, nil
	}
	var canceled *types.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return prolly.StoreTransactionResult{}, dynamoError("transaction", err)
	}
	for _, condition := range conditions {
		current, readErr := s.GetRootManifest(ctx, condition.Name)
		if readErr != nil {
			return prolly.StoreTransactionResult{}, readErr
		}
		if !optionalEqual(current, condition.Expected) {
			return prolly.StoreTransactionResult{Conflict: &prolly.StoreTransactionConflict{
				Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current,
			}}, nil
		}
	}
	return prolly.StoreTransactionResult{}, dynamoError("transaction", err)
}

func (s *Store) CreateTable(ctx context.Context) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	description, err := s.client.DescribeTable(ctx, &awsdynamodb.DescribeTableInput{TableName: &s.options.TableName})
	if err == nil {
		return validateTable(description.Table)
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return dynamoError("describe_table", err)
	}
	_, err = s.client.CreateTable(ctx, &awsdynamodb.CreateTableInput{
		TableName:            &s.options.TableName,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: stringPointer(partitionKeyAttr), AttributeType: types.ScalarAttributeTypeB}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: stringPointer(partitionKeyAttr), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return dynamoError("create_table", err)
		}
	}
	for attempts := 0; attempts < 100; attempts++ {
		output, describeErr := s.client.DescribeTable(ctx, &awsdynamodb.DescribeTableInput{TableName: &s.options.TableName})
		if describeErr == nil && output.Table != nil && output.Table.TableStatus == types.TableStatusActive {
			return validateTable(output.Table)
		}
		if describeErr != nil && !errors.As(describeErr, &notFound) {
			return dynamoError("describe_table", describeErr)
		}
		if err := waitDuration(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
	return &prolly.StoreError{Code: "timeout", Message: "DynamoDB table did not become active", Retryable: true}
}

func (s *Store) DeleteTable(ctx context.Context) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.client.DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: &s.options.TableName})
	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return nil
	}
	return dynamoError("delete_table", err)
}

func (s *Store) ClearNamespace(ctx context.Context) error {
	if len(s.options.KeyPrefix) == 0 {
		return &prolly.StoreError{Code: "invalid_argument", Message: "refusing to clear an empty DynamoDB key prefix"}
	}
	keys, err := s.scanKeys(ctx, s.options.KeyPrefix)
	if err != nil {
		return err
	}
	requests := make([]types.WriteRequest, 0, len(keys))
	for _, key := range keys {
		requests = append(requests, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: s.keyItem(key)}})
	}
	return s.batchWrite(ctx, requests)
}

func (s *Store) get(ctx context.Context, key []byte, operation string) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	output, err := s.client.GetItem(ctx, &awsdynamodb.GetItemInput{
		TableName: &s.options.TableName, Key: s.keyItem(key), ConsistentRead: boolPointer(true),
		ProjectionExpression: stringPointer("#value"), ExpressionAttributeNames: map[string]string{"#value": valueAttr},
	})
	if err != nil {
		return prolly.OptionalBytes{}, dynamoError(operation, err)
	}
	if len(output.Item) == 0 {
		return prolly.MissingBytes(), nil
	}
	value, err := binaryAttribute(output.Item, valueAttr)
	if err != nil {
		return prolly.OptionalBytes{}, err
	}
	return prolly.PresentBytes(value), nil
}

func (s *Store) put(ctx context.Context, key, value []byte, operation string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.client.PutItem(ctx, &awsdynamodb.PutItemInput{TableName: &s.options.TableName, Item: s.item(key, value)})
	return dynamoError(operation, err)
}

func (s *Store) delete(ctx context.Context, key []byte, operation string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.client.DeleteItem(ctx, &awsdynamodb.DeleteItemInput{TableName: &s.options.TableName, Key: s.keyItem(key)})
	return dynamoError(operation, err)
}

func (s *Store) batchWrite(ctx context.Context, requests []types.WriteRequest) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	for start := 0; start < len(requests); start += batchWriteLimit {
		end := min(start+batchWriteLimit, len(requests))
		pending := cloneWriteRequests(requests[start:end])
		for attempt := 0; len(pending) != 0; attempt++ {
			output, err := s.client.BatchWriteItem(ctx, &awsdynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{s.options.TableName: pending}})
			if err != nil {
				return dynamoError("batch_write", err)
			}
			pending = output.UnprocessedItems[s.options.TableName]
			if len(pending) != 0 {
				if attempt+1 >= batchRetryLimit {
					return limitError("DynamoDB batch write left %d requests unprocessed", len(pending))
				}
				if err := waitBackoff(ctx, attempt); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Store) scanKeys(ctx context.Context, prefix []byte) ([][]byte, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	var start map[string]types.AttributeValue
	var keys [][]byte
	for {
		output, err := s.client.Scan(ctx, &awsdynamodb.ScanInput{
			TableName: &s.options.TableName, ConsistentRead: boolPointer(true),
			ProjectionExpression: stringPointer("#pk"), FilterExpression: stringPointer("begins_with(#pk, :prefix)"),
			ExpressionAttributeNames:  map[string]string{"#pk": partitionKeyAttr},
			ExpressionAttributeValues: map[string]types.AttributeValue{":prefix": binaryValue(prefix)},
			ExclusiveStartKey:         start,
		})
		if err != nil {
			return nil, dynamoError("scan", err)
		}
		for _, item := range output.Items {
			key, err := binaryAttribute(item, partitionKeyAttr)
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}
		start = output.LastEvaluatedKey
		if len(start) == 0 {
			return keys, nil
		}
	}
}

func (s *Store) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "DynamoDB client is nil"}
	}
	if strings.TrimSpace(s.options.TableName) == "" {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "DynamoDB table name is empty"}
	}
	return nil
}

func (s *Store) nodeKey(key []byte) []byte         { return s.familyKey(nodeFamily, key) }
func (s *Store) rootKey(key []byte) []byte         { return s.familyKey(rootFamily, key) }
func (s *Store) familyPrefix(family []byte) []byte { return s.familyKey(family, nil) }
func (s *Store) familyKey(family, suffix []byte) []byte {
	result := make([]byte, 0, len(s.options.KeyPrefix)+len(family)+len(suffix))
	result = append(result, s.options.KeyPrefix...)
	result = append(result, family...)
	return append(result, suffix...)
}
func (s *Store) hintKey(namespace, key []byte) []byte {
	result := s.familyPrefix(hintFamily)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(namespace)))
	result = append(result, length[:]...)
	result = append(result, namespace...)
	return append(result, key...)
}

func (s *Store) keyItem(key []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{partitionKeyAttr: binaryValue(key)}
}
func (s *Store) item(key, value []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{partitionKeyAttr: binaryValue(key), valueAttr: binaryValue(value)}
}

func rootCondition(expected prolly.OptionalBytes) (string, map[string]string, map[string]types.AttributeValue) {
	if expected.Present {
		return "#value = :expected", map[string]string{"#value": valueAttr}, map[string]types.AttributeValue{":expected": binaryValue(expected.Value)}
	}
	return "attribute_not_exists(#pk)", map[string]string{"#pk": partitionKeyAttr}, nil
}

func validateTable(table *types.TableDescription) error {
	if table == nil || len(table.KeySchema) != 1 || table.KeySchema[0].AttributeName == nil || *table.KeySchema[0].AttributeName != partitionKeyAttr || table.KeySchema[0].KeyType != types.KeyTypeHash {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "DynamoDB table must use one HASH key named pk"}
	}
	for _, definition := range table.AttributeDefinitions {
		if definition.AttributeName != nil && *definition.AttributeName == partitionKeyAttr && definition.AttributeType == types.ScalarAttributeTypeB {
			return nil
		}
	}
	return &prolly.StoreError{Code: "invalid_configuration", Message: "DynamoDB table pk attribute must be binary"}
}

func binaryValue(value []byte) types.AttributeValue {
	return &types.AttributeValueMemberB{Value: clone(value)}
}
func binaryAttribute(item map[string]types.AttributeValue, name string) ([]byte, error) {
	attribute, ok := item[name]
	if !ok {
		return nil, &prolly.StoreError{Code: "invalid_result", Message: "DynamoDB item missing " + name + " attribute"}
	}
	binary, ok := attribute.(*types.AttributeValueMemberB)
	if !ok {
		return nil, &prolly.StoreError{Code: "invalid_result", Message: "DynamoDB item has non-binary " + name + " attribute"}
	}
	return clone(binary.Value), nil
}

func dynamoError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := "provider_error"
	providerCode := ""
	retryable := false
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		providerCode = apiError.ErrorCode()
		switch providerCode {
		case "ProvisionedThroughputExceededException", "RequestLimitExceeded", "InternalServerError", "ServiceUnavailable", "ThrottlingException":
			retryable = true
		}
	}
	return &prolly.StoreError{Code: code, Message: operation + ": " + err.Error(), Retryable: retryable, ProviderCode: providerCode, Cause: err}
}

func limitError(format string, args ...any) error {
	return &prolly.StoreError{Code: "limit_exceeded", Message: fmt.Sprintf(format, args...)}
}
func waitBackoff(ctx context.Context, attempt int) error {
	return waitDuration(ctx, time.Duration(1<<min(attempt, 6))*10*time.Millisecond)
}
func waitDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}
func clone(value []byte) []byte {
	if value == nil {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
func cloneWriteRequests(value []types.WriteRequest) []types.WriteRequest {
	return append([]types.WriteRequest(nil), value...)
}
func boolPointer(value bool) *bool       { return &value }
func stringPointer(value string) *string { return &value }
