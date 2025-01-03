import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.gson.Gson;

public class SkierClient {
    private static final int TOTAL_REQUESTS = 200000;
    private static final int NUM_THREADS = 32;  // Adjust number of threads
    private static final int NUM_REQUESTS = TOTAL_REQUESTS/NUM_THREADS;  // Each thread sends more requests to reach 200,000 requests
    private static final String SERVER_URL = "http://35.82.154.244:8080/SkiServlets-1.0-SNAPSHOT/skiers";
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failCount = new AtomicInteger(0);
    private static final int MAX_RETRIES = 5;  // Maximum retry attempts

    public static void main(String[] args) throws InterruptedException {
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

                                if (response.statusCode() == 201) {
                                    successCount.incrementAndGet();
                                    requestSent = true;  // Request successful, exit loop
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
        System.out.println("Number of requests: " + TOTAL_REQUESTS);
        System.out.println("Total requests: " + TOTAL_REQUESTS);
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Failed requests: " + failCount.get());
        System.out.println("Total time (ms): " + totalTime);
        System.out.println("Throughput (requests/sec): " + (TOTAL_REQUESTS / (totalTime / 1000.0)));
    }
}
