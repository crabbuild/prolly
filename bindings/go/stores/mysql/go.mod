module build.crab/prolly-go/stores/mysql

go 1.24.0

require (
	build.crab/prolly-go v0.0.0
	github.com/go-sql-driver/mysql v1.10.0
)

require filippo.io/edwards25519 v1.2.0 // indirect

replace build.crab/prolly-go => ../..
