module build.crab/prolly-go/stores/cosmosdb

go 1.25.0

require (
	build.crab/prolly-go v0.0.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.22.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.5.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace build.crab/prolly-go => ../..
