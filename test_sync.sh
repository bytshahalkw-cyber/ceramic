#!/bin/bash

echo "=== 1. Checking Node Health ==="
curl -s http://localhost:7001/api/v0/healthcheck && echo " -> Node 1 OK"
curl -s http://localhost:7002/api/v0/healthcheck && echo " -> Node 2 OK"
curl -s http://localhost:7003/api/v0/healthcheck && echo " -> Node 3 OK"

echo ""
echo "=== 2. Ceramic 3-Node Cluster is Ready for Cloud Deployment ==="

