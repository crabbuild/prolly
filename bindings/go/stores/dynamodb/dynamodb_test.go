package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDynamoDBConformance(t *testing.T) {
	endpoint := os.Getenv("PROLLY_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("PROLLY_DYNAMODB_ENDPOINT is not set")
	}
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := awsdynamodb.NewFromConfig(cfg, func(options *awsdynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
	store := New(client, Options{
		TableName: fmt.Sprintf("prolly_go_%d", time.Now().UnixNano()),
		KeyPrefix: []byte("prolly:test:"),
	})
	if err := store.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.DeleteTable(context.Background()) })
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}

func TestTransactionLimitIsPreflighted(t *testing.T) {
	store := New(nil, Options{TableName: "unused"})
	nodes := make([]prolly.NodeMutation, 101)
	for index := range nodes {
		nodes[index] = prolly.UpsertNode([]byte{byte(index)}, []byte("value"))
	}
	_, err := store.CommitTransaction(context.Background(), nodes, nil, nil)
	var storeErr *prolly.StoreError
	if err == nil || !errors.As(err, &storeErr) || storeErr.Code != "limit_exceeded" {
		t.Fatalf("error = %#v", err)
	}
}

func TestBinaryValuePreservesPresentEmpty(t *testing.T) {
	value := binaryValue([]byte{})
	binary := value.(*types.AttributeValueMemberB)
	if binary.Value == nil || len(binary.Value) != 0 {
		t.Fatalf("binary value = %#v", binary.Value)
	}
}

func TestServiceLimitsAreChunked(t *testing.T) {
	client := &recordingClient{}
	store := New(client, Options{TableName: "table"})
	keys := make([][]byte, 101)
	for index := range keys {
		keys[index] = []byte{byte(index)}
	}
	if _, err := store.BatchGetNodesOrdered(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(client.batchGetSizes) != "[100 1]" {
		t.Fatalf("batch get sizes = %v", client.batchGetSizes)
	}
	mutations := make([]prolly.NodeMutation, 26)
	for index := range mutations {
		mutations[index] = prolly.UpsertNode([]byte{byte(index)}, []byte("v"))
	}
	if err := store.BatchNodes(context.Background(), mutations); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(client.batchWriteSizes) != "[25 1]" {
		t.Fatalf("batch write sizes = %v", client.batchWriteSizes)
	}
}

type recordingClient struct {
	Client
	batchGetSizes   []int
	batchWriteSizes []int
}

func (c *recordingClient) BatchGetItem(_ context.Context, input *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
	c.batchGetSizes = append(c.batchGetSizes, len(input.RequestItems["table"].Keys))
	return &awsdynamodb.BatchGetItemOutput{}, nil
}

func (c *recordingClient) BatchWriteItem(_ context.Context, input *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
	c.batchWriteSizes = append(c.batchWriteSizes, len(input.RequestItems["table"]))
	return &awsdynamodb.BatchWriteItemOutput{}, nil
}
