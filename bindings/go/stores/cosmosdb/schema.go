package cosmosdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	prolly "build.crab/prolly-go"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const SchemaVersion uint32 = 1

// EnsureDatabaseAndContainer creates the named resources if absent and
// validates the required /kind partition key. It never deletes data.
func EnsureDatabaseAndContainer(ctx context.Context, client *azcosmos.Client, databaseID, containerID string) (*azcosmos.ContainerClient, error) {
	if client == nil || databaseID == "" || containerID == "" {
		return nil, &prolly.StoreError{Code: "invalid_configuration", Message: "Cosmos client, database ID, and container ID are required"}
	}
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: databaseID}, nil)
	if err != nil && !hasStatus(err, http.StatusConflict) {
		return nil, cosmosError("create_database", err)
	}
	database, err := client.NewDatabase(databaseID)
	if err != nil {
		return nil, err
	}
	properties := azcosmos.ContainerProperties{ID: containerID, PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Kind: azcosmos.PartitionKeyKindHash, Paths: []string{"/kind"}}}
	_, err = database.CreateContainer(ctx, properties, nil)
	if err != nil && !hasStatus(err, http.StatusConflict) {
		return nil, cosmosError("create_container", err)
	}
	container, err := database.NewContainer(containerID)
	if err != nil {
		return nil, err
	}
	response, err := container.Read(ctx, nil)
	if err != nil {
		return nil, cosmosError("read_container", err)
	}
	paths := response.ContainerProperties.PartitionKeyDefinition.Paths
	if len(paths) != 1 || paths[0] != "/kind" {
		return nil, &prolly.StoreError{Code: "invalid_configuration", Message: fmt.Sprintf("Cosmos container partition paths must be [/kind], got %v", paths)}
	}
	return container, nil
}

func hasStatus(err error, status int) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == status
}
