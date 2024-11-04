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

@WebServlet(value = "/skiers/*")
public class SkierServlet extends HttpServlet {

    private static final String RABBITMQ_HOST = "54.202.193.198";
    private static final int RABBITMQ_PORT = 5672;
    private static final String RABBITMQ_USERNAME = "ratu";
    private static final String RABBITMQ_PASSWORD = "548919";
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final int CHANNEL_POOL_SIZE = 10;

    private Connection connection;
    private BlockingQueue<Channel> channelPool;
    private final Gson gson = new Gson();

    @Override
    public void init() throws ServletException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
            factory.setPort(RABBITMQ_PORT);
            factory.setUsername(RABBITMQ_USERNAME);
            factory.setPassword(RABBITMQ_PASSWORD);
            connection = factory.newConnection();

            channelPool = new ArrayBlockingQueue<>(CHANNEL_POOL_SIZE);
            for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
                Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                channelPool.add(channel);
            }
        } catch (Exception e) {
            throw new ServletException("Failed to initialize RabbitMQ connection", e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json");

        try {
            LiftRide liftRide = gson.fromJson(req.getReader(), LiftRide.class);
            if (!isPayloadValid(liftRide)) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                res.getWriter().write("{\"error\": \"Invalid lift ride data\"}");
                return;
            }

            String message = gson.toJson(liftRide);
            Channel channel = channelPool.take();
            try {
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                res.setStatus(HttpServletResponse.SC_CREATED);
                res.getWriter().write("{\"status\": \"Lift ride recorded successfully.\"}");
            } finally {
                channelPool.offer(channel);
            }
        } catch (JsonParseException e) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write("{\"error\": \"Invalid JSON format\"}");
        } catch (Exception e) {
            res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res.getWriter().write("{\"error\": \"Failed to record lift ride\"}");
        }
    }

    @Override
    public void destroy() {
        try {
            for (Channel channel : channelPool) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            // Log or handle exception if necessary
        }
    }

    private boolean isPayloadValid(LiftRide liftRide) {
        return liftRide != null &&
                liftRide.getSkierID() >= 1 && liftRide.getSkierID() <= 100000 &&
                liftRide.getResortID() >= 1 && liftRide.getResortID() <= 10 &&
                liftRide.getLiftID() >= 1 && liftRide.getLiftID() <= 40 &&
                liftRide.getTime() >= 1 && liftRide.getTime() <= 360;
    }
}
