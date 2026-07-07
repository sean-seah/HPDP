# Start Kafka (if not already running) and create the app-reviews topic.
$ErrorActionPreference = "Stop"

$containerName = "kafka"
$topic = "app-reviews"

$existing = docker ps -a --filter "name=^/${containerName}$" --format "{{.Names}}"
if (-not $existing) {
    Write-Host "Starting Kafka container..."
    docker run -d --name $containerName -p 9092:9092 apache/kafka:latest
} else {
    $running = docker ps --filter "name=^/${containerName}$" --format "{{.Names}}"
    if (-not $running) {
        Write-Host "Starting existing Kafka container..."
        docker start $containerName
    } else {
        Write-Host "Kafka container already running."
    }
}

Write-Host "Waiting for Kafka to be ready..."
Start-Sleep -Seconds 10

Write-Host "Ensuring topic '$topic' exists..."
docker exec $containerName /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create --if-not-exists --topic $topic `
    --partitions 1 --replication-factor 1

Write-Host "Done. Kafka is ready on localhost:9092 with topic '$topic'."
