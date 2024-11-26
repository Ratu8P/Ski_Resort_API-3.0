import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SkierClient {
    private static final int TOTAL_REQUESTS = 200000;
    private static final int NUM_THREADS = 256;  // Adjust number of threads
    private static final int NUM_REQUESTS = TOTAL_REQUESTS / NUM_THREADS;  // Each thread sends more requests to reach 200,000
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failCount = new AtomicInteger(0);
    private static final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());  // Store latencies

    public static void main(String[] args) throws InterruptedException {
        // Record start time
        long startTime = System.currentTimeMillis();

        // Create a thread pool, using a fixed size to avoid excessive concurrency
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < NUM_REQUESTS; j++) {
                        LiftRide ride = LiftRideGenerator.generateLiftRide();
                        HttpClientService.sendRequest(ride, successCount, failCount, latencies);
                    }
                } catch (Exception e) {
                    System.out.println("Error in thread: " + e.getMessage());
                }
            });
        }

        // Shut down thread pool and wait for all tasks to complete
        executor.shutdown();
        boolean tasksCompleted = executor.awaitTermination(10, TimeUnit.MINUTES);

        if (!tasksCompleted) {
            System.out.println("Timeout occurred before all tasks completed.");
        }

        // Record end time and calculate total duration
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // Print statistics
        System.out.println("Number of threads: " + NUM_THREADS);
        System.out.println("Total requests: " + TOTAL_REQUESTS);
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Failed requests: " + failCount.get());
        System.out.println("Total time (ms): " + totalTime);
        System.out.println("Throughput (requests/sec): " + (TOTAL_REQUESTS / (totalTime / 1000.0)));

        // Calculate and display latency statistics
        calculateLatencyStatistics();
    }

    // Method to calculate and display latency statistics
    private static void calculateLatencyStatistics() {
        // Sort latencies to calculate percentiles and median
        List<Long> sortedLatencies = latencies.stream().sorted().toList();

        if (sortedLatencies.isEmpty()) {
            System.out.println("No latencies recorded. Please check your request flow.");
            return;
        }

        long sum = sortedLatencies.stream().mapToLong(Long::longValue).sum();
        double mean = sum / (double) sortedLatencies.size();
        long median = sortedLatencies.get(sortedLatencies.size() / 2);
        long p99 = sortedLatencies.get((int) (sortedLatencies.size() * 0.99));
        long min = sortedLatencies.get(0);
        long max = sortedLatencies.get(sortedLatencies.size() - 1);

        // Print statistics
        System.out.println("Mean response time (ms): " + mean);
        System.out.println("Median response time (ms): " + median);
        System.out.println("99th percentile response time (ms): " + p99);
        System.out.println("Min response time (ms): " + min);
        System.out.println("Max response time (ms): " + max);
    }
}
