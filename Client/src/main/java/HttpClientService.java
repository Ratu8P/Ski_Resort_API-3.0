import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.gson.Gson;

public class HttpClientService {
    private static final int MAX_RETRIES = 5;  // Maximum retry attempts
    private static final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public static void sendRequest(LiftRide ride, AtomicInteger successCount, AtomicInteger failCount, List<Long> latencies) {
        int retries = 0;
        long requestStartTime = System.currentTimeMillis();

        // Build POST request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://35.82.154.244:8080/Server_war/skiers"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(new Gson().toJson(ride)))
                .build();

        while (retries < MAX_RETRIES) {
            try {
                // Send request and get response
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long requestEndTime = System.currentTimeMillis();
                long latency = requestEndTime - requestStartTime;  // Calculate latency

                if (response.statusCode() == 201) {
                    successCount.incrementAndGet();
                    latencies.add(latency);  // Add latency to the list
                    return;  // Exit the method after success
                } else {
                    retries++;
                    System.out.println("Request failed with status code: " + response.statusCode() + ", Retrying... (" + retries + ")");
                }
            } catch (Exception e) {
                retries++;
                System.out.println("Error sending request: " + e.getMessage() + ", Retrying... (" + retries + ")");
            }
        }

        failCount.incrementAndGet();  // Increment failure count after retries exceed MAX_RETRIES
    }
}
