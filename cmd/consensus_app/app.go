package main

import (
	"fmt"
	"log"
	"os"

	"github.com/blockchain/consensus/node"
)

const defaultConfigFile = "node_config.yaml" // Tên file cấu hình mặc định

func main() {
	var configPath string

	if len(os.Args) < 2 {
		fmt.Printf("Using default config file: %s\n", defaultConfigFile)
		configPath = defaultConfigFile
	} else {
		configPath = os.Args[1]
	}

	err := node.StartNode(configPath)
	if err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}
}
