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
    private static final int INITIAL_NUM_THREADS = 32;
    private static final String SERVER_URL = "http://35.82.154.244:8080/SkiServlets-1.0-SNAPSHOT/skiers";
    private static final int MAX_RETRIES = 5;

    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failCount = new AtomicInteger(0);
    private static final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> requestLogs = Collections.synchronizedList(new ArrayList<>());

    private static final Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(INITIAL_NUM_THREADS);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < INITIAL_NUM_THREADS; i++) {
            executor.submit(() -> sendRequests(client));
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

        long totalTime = System.currentTimeMillis() - startTime;
        logStatistics(totalTime);
        saveRequestLog();
        calculateLatencyStatistics();
    }

    private static void sendRequests(HttpClient client) {
        for (int j = 0; j < TOTAL_REQUESTS / INITIAL_NUM_THREADS; j++) {
            LiftRide ride = LiftRideGenerator.generateLiftRide();
            sendRequestWithRetry(client, ride);
        }
    }

    private static void sendRequestWithRetry(HttpClient client, LiftRide ride) {
        int retries = 0;
        boolean requestSent = false;
        long requestStartTime = System.currentTimeMillis();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SERVER_URL))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(ride)))
                .build();

        while (!requestSent && retries < MAX_RETRIES) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                handleResponse(response, requestStartTime);
                requestSent = response.statusCode() == 201;
            } catch (Exception e) {
                System.err.println("Error sending request: " + e.getMessage() + ", retrying...");
                retries++;
                try { Thread.sleep(100L * retries); } catch (InterruptedException ignored) {}
            }
        }

        if (!requestSent) failCount.incrementAndGet();
    }

    private static void handleResponse(HttpResponse<String> response, long requestStartTime) {
        long latency = System.currentTimeMillis() - requestStartTime;
        latencies.add(latency);
        requestLogs.add(requestStartTime + ",POST," + latency + "," + response.statusCode());
        if (response.statusCode() == 201) {
            successCount.incrementAndGet();
        } else {
            System.err.println("Request failed with status code: " + response.statusCode());
        }
    }

    private static void logStatistics(long totalTime) {
        System.out.printf("Threads: %d, Total requests: %d, Successful: %d, Failed: %d\n",
                INITIAL_NUM_THREADS, TOTAL_REQUESTS, successCount.get(), failCount.get());
        System.out.printf("Total time: %d ms, Throughput: %.2f requests/sec\n",
                totalTime, TOTAL_REQUESTS / (totalTime / 1000.0));
    }

    private static void saveRequestLog() {
        try (PrintWriter writer = new PrintWriter(new FileWriter("request_log.csv"))) {
            writer.println("start_time,request_type,latency,response_code");
            for (String log : requestLogs) writer.println(log);
        } catch (IOException e) {
            System.err.println("Error writing request log: " + e.getMessage());
        }
    }

    private static void calculateLatencyStatistics() {
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
