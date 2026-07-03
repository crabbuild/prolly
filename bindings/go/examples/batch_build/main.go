package main

import (
	"log"

	"build.crab/prolly-go/examples/internal/cookbook"
)

func main() {
	if err := cookbook.BatchBuild(); err != nil {
		log.Fatal(err)
	}
}
