package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/womentechies26/aqe/backend/internal/app"
)

func main() {
	port := os.Getenv("AQE_PORT")
	if port == "" {
		port = "8088"
	}

	dbPath := os.Getenv("AQE_DUCKDB_PATH")
	if dbPath == "" {
		dbPath = filepath.Join(".", "aqe.duckdb")
	}

	server, err := app.NewServer(dbPath)
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}
	defer server.Close()

	addr := "127.0.0.1:" + port
	log.Printf("AQE backend listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Routes()); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
