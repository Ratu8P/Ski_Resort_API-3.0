import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

@WebServlet(value = "/skiers/*")
public class SkierServlet extends HttpServlet {

    private static final String RABBITMQ_HOST = "35.82.154.244" ;
    private static final int RABBITMQ_PORT = 15672;
    private static final String RABBITMQ_USERNAME = "guest";
    private static final String RABBITMQ_PASSWORD = "guest";
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final int CHANNEL_POOL_SIZE = 10;

    private Connection connection;
    private BlockingQueue<Channel> channelPool;
    private final Gson gson = new Gson();
    private final Logger logger = Logger.getLogger(SkierServlet.class.getName());

    @Override
    public void init() throws ServletException {
        super.init();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
            factory.setPort(RABBITMQ_PORT);
            factory.setUsername(RABBITMQ_USERNAME);
            factory.setPassword(RABBITMQ_PASSWORD);
            connection = factory.newConnection();

            // Initialize a pool of channels for concurrent requests
            channelPool = new ArrayBlockingQueue<>(CHANNEL_POOL_SIZE);
            for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
                channelPool.add(connection.createChannel());
            }
        } catch (Exception e) {
            logger.severe("Error initializing RabbitMQ connection: " + e.getMessage());
            throw new ServletException(e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("text/plain");
        String urlPath = req.getPathInfo();

        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("Missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("Invalid URL");
        } else {
            res.setStatus(HttpServletResponse.SC_OK);
            res.getWriter().write("It works! GET request handled successfully.");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json");

        // Validate JSON payload
        LiftRide liftRide;
        try {
            liftRide = gson.fromJson(req.getReader(), LiftRide.class);
            if (!isPayloadValid(liftRide)) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                res.getWriter().write("{\"error\": \"Invalid lift ride data\"}");
                return;
            }
        } catch (JsonParseException e) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write("{\"error\": \"Invalid JSON format\"}");
            return;
        }

        // Publish to RabbitMQ
        String message = gson.toJson(liftRide);
        Channel channel = null;
        try {
            channel = channelPool.take(); // Take a channel from the pool
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            res.setStatus(HttpServletResponse.SC_CREATED);
            res.getWriter().write("{ \"status\": \"Lift ride recorded successfully.\" }");
        } catch (Exception e) {
            logger.severe("Error publishing message to RabbitMQ: " + e.getMessage());
            res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res.getWriter().write("{\"error\": \"Failed to record lift ride\"}");
        } finally {
            if (channel != null) {
                channelPool.offer(channel); // Return channel to the pool
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            logger.severe("Error closing RabbitMQ connection: " + e.getMessage());
        }
    }

    private boolean isUrlValid(String[] urlPath) {
        // Validate URL format: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlPath.length == 8) {
            try {
                Integer.parseInt(urlPath[1]); // resortID
                Integer.parseInt(urlPath[3]); // seasonID
                Integer.parseInt(urlPath[5]); // dayID
                Integer.parseInt(urlPath[7]); // skierID
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    private boolean isPayloadValid(LiftRide liftRide) {
        // Ensure liftRide fields are valid (e.g., ID ranges)
        return liftRide != null &&
                liftRide.getSkierID() >= 1 && liftRide.getSkierID() <= 100000 &&
                liftRide.getResortID() >= 1 && liftRide.getResortID() <= 10 &&
                liftRide.getLiftID() >= 1 && liftRide.getLiftID() <= 40 &&
                liftRide.getTime() >= 1 && liftRide.getTime() <= 360;
    }
}
