package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	mainQueue       = "tasks:queue"
	dlq             = "tasks:dlq"
	processingQueue = "tasks:processing" // For reliability
	maxRetries      = 3
)

// Task represents a job to be processed
type Task struct {
	ID        string    `json:"id"`
	Data      string    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
	Retries   int       `json:"retries"`
	LastError string    `json:"last_error,omitempty"`
}

// RedisClient wraps the Redis client with DLQ-specific operations
type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Test connection
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisClient{client: client}, nil
}

// EnqueueTask adds a new task to the main queue
func (rc *RedisClient) EnqueueTask(ctx context.Context, task Task) error {
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	return rc.client.LPush(ctx, mainQueue, taskJSON).Err()
}

// DequeueTask gets a task from the queue with reliability
func (rc *RedisClient) DequeueTask(ctx context.Context) (*Task, error) {
	// Move from main queue to processing queue atomically
	script := `
		local val = redis.call('RPOP', KEYS[1])
		if val then
			redis.call('LPUSH', KEYS[2], val)
			return val
		end
		return nil
	`

	res, err := rc.client.Eval(ctx, script, []string{mainQueue, processingQueue}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Queue is empty
		}
		return nil, fmt.Errorf("error dequeuing task: %w", err)
	}

	if res == nil {
		return nil, nil // Queue is empty
	}

	taskJSON, ok := res.(string)
	if !ok {
		return nil, errors.New("unexpected response type from Redis")
	}

	var task Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// MarkTaskComplete removes a task from the processing queue
func (rc *RedisClient) MarkTaskComplete(ctx context.Context, task Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Remove the task from the processing queue
	return rc.client.LRem(ctx, processingQueue, 1, taskJSON).Err()
}

// MoveTaskToDLQ moves a failed task to the dead letter queue
func (rc *RedisClient) MoveTaskToDLQ(ctx context.Context, task Task, err error) error {
	// Update task with error information
	task.LastError = err.Error()
	task.Retries++

	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Remove from processing queue
	if err := rc.client.LRem(ctx, processingQueue, 1, taskJSON).Err(); err != nil {
		return fmt.Errorf("failed to remove task from processing queue: %w", err)
	}

	// If we haven't exceeded max retries, put it back in the main queue
	if task.Retries < maxRetries {
		return rc.client.LPush(ctx, mainQueue, taskJSON).Err()
	}

	// Otherwise, move to DLQ
	return rc.client.LPush(ctx, dlq, taskJSON).Err()
}

// GetTasksFromDLQ retrieves all tasks in the DLQ
func (rc *RedisClient) GetTasksFromDLQ(ctx context.Context) ([]Task, error) {
	// Get all tasks from DLQ without removing them
	result, err := rc.client.LRange(ctx, dlq, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks from DLQ: %w", err)
	}

	tasks := make([]Task, 0, len(result))
	for _, taskJSON := range result {
		var task Task
		if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
			return nil, fmt.Errorf("failed to unmarshal task: %w", err)
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// RetryTaskFromDLQ moves a task from DLQ back to the main queue
func (rc *RedisClient) RetryTaskFromDLQ(ctx context.Context, taskID string) error {
	// Get all tasks from DLQ
	tasks, err := rc.GetTasksFromDLQ(ctx)
	if err != nil {
		return err
	}

	// Find the task with the matching ID
	for _, task := range tasks {
		if task.ID == taskID {
			// Reset retries and error
			task.Retries = 0
			task.LastError = ""

			taskJSON, err := json.Marshal(task)
			if err != nil {
				return fmt.Errorf("failed to marshal task: %w", err)
			}

			// Remove from DLQ and add to main queue in a transaction
			pipe := rc.client.TxPipeline()
			pipe.LRem(ctx, dlq, 1, taskJSON)
			pipe.LPush(ctx, mainQueue, taskJSON)
			_, err = pipe.Exec(ctx)
			return err
		}
	}

	return fmt.Errorf("task with ID %s not found in DLQ", taskID)
}

// PurgeTaskFromDLQ removes a task from DLQ without reprocessing
func (rc *RedisClient) PurgeTaskFromDLQ(ctx context.Context, taskID string) error {
	// Get all tasks from DLQ
	tasks, err := rc.GetTasksFromDLQ(ctx)
	if err != nil {
		return err
	}

	// Find the task with the matching ID
	for _, task := range tasks {
		if task.ID == taskID {
			taskJSON, err := json.Marshal(task)
			if err != nil {
				return fmt.Errorf("failed to marshal task: %w", err)
			}

			// Remove from DLQ
			return rc.client.LRem(ctx, dlq, 1, taskJSON).Err()
		}
	}

	return fmt.Errorf("task with ID %s not found in DLQ", taskID)
}

// PurgeAllDLQ removes all tasks from DLQ
func (rc *RedisClient) PurgeAllDLQ(ctx context.Context) error {
	return rc.client.Del(ctx, dlq).Err()
}

// Worker processes tasks
type Worker struct {
	id          int
	redisClient *RedisClient
	processFunc func(Task) error
	wg          *sync.WaitGroup
	stop        chan struct{}
}

// NewWorker creates a new task worker
func NewWorker(id int, rc *RedisClient, processFunc func(Task) error, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:          id,
		redisClient: rc,
		processFunc: processFunc,
		wg:          wg,
		stop:        make(chan struct{}),
	}
}

// Start begins the worker's processing loop
func (w *Worker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		log.Printf("Worker %d started", w.id)

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Worker %d shutting down (context cancelled)", w.id)
				return
			case <-w.stop:
				log.Printf("Worker %d shutting down (stopped)", w.id)
				return
			case <-ticker.C:
				w.processNextTask(ctx)
			}
		}
	}()
}

// Stop signals the worker to stop
func (w *Worker) Stop() {
	close(w.stop)
}

// processNextTask processes the next available task
func (w *Worker) processNextTask(ctx context.Context) {
	task, err := w.redisClient.DequeueTask(ctx)
	if err != nil {
		log.Printf("Worker %d: Error dequeuing task: %v", w.id, err)
		return
	}

	if task == nil {
		// No tasks in queue
		return
	}

	log.Printf("Worker %d: Processing task %s (attempt %d)", w.id, task.ID, task.Retries+1)

	// Process the task
	err = w.processFunc(*task)
	if err != nil {
		log.Printf("Worker %d: Failed to process task %s: %v", w.id, task.ID, err)

		// Move to DLQ if processing failed
		if moveErr := w.redisClient.MoveTaskToDLQ(ctx, *task, err); moveErr != nil {
			log.Printf("Worker %d: Failed to move task %s to DLQ: %v", w.id, task.ID, moveErr)
		}
		return
	}

	// Mark task as complete
	if err := w.redisClient.MarkTaskComplete(ctx, *task); err != nil {
		log.Printf("Worker %d: Failed to mark task %s as complete: %v", w.id, task.ID, err)
	} else {
		log.Printf("Worker %d: Successfully processed task %s", w.id, task.ID)
	}
}

// DLQManager manages tasks in the dead letter queue
type DLQManager struct {
	redisClient *RedisClient
}

func NewDLQManager(rc *RedisClient) *DLQManager {
	return &DLQManager{redisClient: rc}
}

// ShowFailedTasks displays all tasks in the DLQ
func (dm *DLQManager) ShowFailedTasks(ctx context.Context) {
	tasks, err := dm.redisClient.GetTasksFromDLQ(ctx)
	if err != nil {
		log.Printf("Failed to get tasks from DLQ: %v", err)
		return
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks in DLQ")
		return
	}

	fmt.Println("Tasks in DLQ:")
	fmt.Println("------------")
	for i, task := range tasks {
		fmt.Printf("%d. ID: %s, Data: %s, Failed: %d times, Last Error: %s\n",
			i+1, task.ID, task.Data, task.Retries, task.LastError)
	}
}

// RetryAllTasks moves all tasks from DLQ back to the main queue
func (dm *DLQManager) RetryAllTasks(ctx context.Context) error {
	tasks, err := dm.redisClient.GetTasksFromDLQ(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tasks from DLQ: %w", err)
	}

	for _, task := range tasks {
		task.Retries = 0
		task.LastError = ""

		taskJSON, err := json.Marshal(task)
		if err != nil {
			return fmt.Errorf("failed to marshal task: %w", err)
		}

		pipe := dm.redisClient.client.TxPipeline()
		pipe.LRem(ctx, dlq, 1, taskJSON)
		pipe.LPush(ctx, mainQueue, taskJSON)
		_, err = pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to retry task %s: %w", task.ID, err)
		}

		log.Printf("Task %s moved back to main queue for retry", task.ID)
	}

	log.Printf("All %d tasks moved from DLQ to main queue", len(tasks))
	return nil
}

// SimulateTaskProcessor simulates a task processor that might fail
func SimulateTaskProcessor(task Task) error {
	// Simulate some processing time
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

	// 30% chance of failure
	if rand.Float32() < 0.3 {
		return errors.New("simulated random processing failure")
	}
	return nil
}

func main() {
	log.Println("Starting Redis DLQ pattern demo")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to Redis
	redisClient, err := NewRedisClient("localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Create a wait group for workers
	var wg sync.WaitGroup

	// Start some workers
	numWorkers := 3
	workers := make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = NewWorker(i+1, redisClient, SimulateTaskProcessor, &wg)
		workers[i].Start(ctx)
	}

	// Create a DLQ manager
	dlqManager := NewDLQManager(redisClient)

	// Producer: Generate some sample tasks
	go func() {
		for i := 1; i <= 20; i++ {
			task := Task{
				ID:   fmt.Sprintf("task-%d", i),
				Data: fmt.Sprintf("Sample task data %d", i),
			}
			if err := redisClient.EnqueueTask(ctx, task); err != nil {
				log.Printf("Failed to enqueue task: %v", err)
			} else {
				log.Printf("Enqueued task %s", task.ID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Set up DLQ monitor
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				dlqManager.ShowFailedTasks(ctx)
			}
		}
	}()

	// Wait for Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()

	// Stop workers
	for _, worker := range workers {
		worker.Stop()
	}

	// Wait for all workers to finish
	wg.Wait()

	// Show final DLQ state
	dlqManager.ShowFailedTasks(ctx)

	// Example: Retry all DLQ tasks
	fmt.Println("\nRetrying all tasks in DLQ...")
	if err := dlqManager.RetryAllTasks(ctx); err != nil {
		log.Printf("Failed to retry tasks: %v", err)
	}

	log.Println("Demo completed")
}
