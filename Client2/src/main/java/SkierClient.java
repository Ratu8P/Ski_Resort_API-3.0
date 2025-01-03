import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.gson.Gson;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SkierClient {
    private static final int TOTAL_REQUESTS = 200000;
    private static final int NUM_THREADS = 32;  // Adjust number of threads
    private static final int NUM_REQUESTS = TOTAL_REQUESTS/NUM_THREADS;  // Each thread sends more requests to reach 200,000
    private static final String SERVER_URL = "http://35.82.154.244:8080/SkiServlets-1.0-SNAPSHOT/skiers";
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failCount = new AtomicInteger(0);
    private static final int MAX_RETRIES = 5;  // Maximum retry attempts
    private static final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());  // Store latencies
    private static final List<String> requestLogs = Collections.synchronizedList(new ArrayList<>());  // Store request logs

    public static void main(String[] args) throws InterruptedException, IOException {
        // Create a global HttpClient instance to reuse connections
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        // Create a thread pool, using a fixed size to avoid excessive concurrency
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Record start time
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < NUM_REQUESTS; j++) {
                        LiftRide ride = LiftRideGenerator.generateLiftRide();
                        boolean requestSent = false;
                        int retries = 0;
                        long requestStartTime = System.currentTimeMillis();

                        // Build POST request
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(SERVER_URL))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(new Gson().toJson(ride)))
                                .build();

                        // Retry mechanism
                        while (!requestSent && retries < MAX_RETRIES) {
                            try {
                                // Send request and get response
                                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                                long requestEndTime = System.currentTimeMillis();
                                long latency = requestEndTime - requestStartTime;  // Calculate latency

                                if (response.statusCode() == 201) {
                                    successCount.incrementAndGet();
                                    requestSent = true;  // Request successful, exit loop
                                    latencies.add(latency);  // Add latency to the list
                                    requestLogs.add(requestStartTime + ",POST," + latency + "," + response.statusCode());  // Log request
                                } else {
                                    retries++;
                                    System.out.println("Request failed with status code: " + response.statusCode() + ", Retrying... (" + retries + ")");
                                }
                            } catch (Exception e) {
                                retries++;
                                System.out.println("Error sending request: " + e.getMessage() + ", Retrying... (" + retries + ")");
                            }
                        }

                        if (!requestSent) {
                            failCount.incrementAndGet();  // If retries fail, count as failed request
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error in thread: " + e.getMessage());
                }
            });
        }

        // Shut down thread pool and wait for all tasks to complete
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

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
        System.out.println("      ");

        // Save request log to a CSV file
        try (PrintWriter writer = new PrintWriter(new FileWriter("request_log.csv"))) {
            writer.println("start_time,request_type,latency,response_code");
            for (String log : requestLogs) {
                writer.println(log);
            }
        }

        // Calculate and display latency statistics
        calculateLatencyStatistics();
    }

    // Method to calculate and display latency statistics
    private static void calculateLatencyStatistics() {
        // Sort latencies to calculate percentiles and median
        List<Long> sortedLatencies = latencies.stream().sorted().toList();

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
