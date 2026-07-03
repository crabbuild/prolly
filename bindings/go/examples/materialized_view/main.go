package main

import (
	"log"

	"build.crab/prolly-go/examples/internal/cookbook"
)

func main() {
	if err := cookbook.MaterializedView(); err != nil {
		log.Fatal(err)
	}
}
