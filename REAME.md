# Redis DLQ Pattern in Go

This proof of concept demonstrates implementing a Dead Letter Queue (DLQ) pattern using Redis and Go. The implementation shows how to handle message failures gracefully in a distributed system.

## Key Features

- **Main Queue**: Primary queue for tasks to be processed
- **Processing Queue**: Intermediate queue for reliability during processing
- **Dead Letter Queue (DLQ)**: Destination for messages that fail processing
- **Retry Mechanism**: Configurable retry attempts before moving to DLQ
- **DLQ Management**: Tools to inspect, retry, or purge failed messages
- **Worker Pool**: Concurrent processing of messages

## Architecture

```
┌────────────┐     ┌─────────────┐     ┌────────────┐
│            │     │             │     │            │
│ Main Queue │────▶│ Processing  │────▶│ Successful │
│            │     │   Queue     │     │ Processing │
└────────────┘     └─────────────┘     └────────────┘
       ▲                  │
       │                  │
       │                  ▼
┌────────────┐     ┌─────────────┐
│            │     │             │
│    DLQ     │◀────│   Failed    │
│ Management │     │ Processing  │
└────────────┘     └─────────────┘
```

## Implementation Details

### Message Processing Flow

1. **Enqueue**: Messages are added to the main queue
2. **Dequeue**: Workers move messages to processing queue
3. **Process**: Worker attempts to process the message
4. **Success Path**: Remove from processing queue
5. **Failure Path**: 
   - If retries < max: Return to main queue with incremented retry count
   - If retries = max: Move to DLQ

### Reliability Features

- **Atomic operations**: Using Redis transactions for queue operations
- **Processing queue**: Ensures no message loss during processing
- **Error tracking**: Captures specific error information in failed messages
- **Metrics**: Tracks retry counts and failure reasons

### DLQ Management Tools

- **Inspection**: View all messages in DLQ with their error details
- **Retry**: Move specific or all messages back to main queue
- **Purge**: Remove messages from DLQ permanently

## Setup Instructions

### Prerequisites

- Go 1.18+
- Docker and Docker Compose (for Redis)

### Running the Demo

1. Start Redis:
   ```
   docker-compose up -d
   ```

2. Build and run the application:
   ```
   go mod init redisdlq
   go mod tidy  # To install dependencies
   go run main.go
   ```

3. Access Redis Commander UI:
   ```
   http://localhost:8081
   ```

## Key Components

- **RedisClient**: Wraps Redis operations with DLQ-specific functionality
- **Worker**: Processes messages from the queue
- **DLQManager**: Provides tools for managing the DLQ

## Production Considerations

For a production implementation, consider these enhancements:

1. **Monitoring**: Add Prometheus metrics for queue sizes and processing rates
2. **Alerting**: Set up alerts for DLQ growth
3. **UI**: Create a management UI for DLQ inspection and retries
4. **Scheduled Retries**: Implement time-based retry strategies
5. **Message TTL**: Add expiration for messages in DLQ
6. **Circuit Breaking**: Prevent overloading systems during mass failures
7. **Enhanced Logging**: Structured logging with correlation IDs
8. **Redis Clustering**: For high availability and redundancy

## Demo Explanation

The demo creates:
- A pool of workers processing tasks concurrently
- A producer generating sample tasks
- A periodic DLQ monitor showing failed tasks
- A final retry of all failed tasks

The demo uses intentional random failures to demonstrate the DLQ mechanism.
