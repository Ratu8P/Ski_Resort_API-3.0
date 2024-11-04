import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;    

public class LiftRideConsumer {
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final String RABBITMQ_HOST = "54.202.193.198";
    private static final String RABBITMQ_USERNAME = "ratu";
    private static final String RABBITMQ_PASSWORD = "548919";
    private static final int RABBITMQ_PORT = 5672;
    private static final int NUM_CONSUMER_THREADS = 10;

    private final ConcurrentHashMap<Integer, Integer> liftRideCounts = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();
    private final Logger logger = Logger.getLogger(LiftRideConsumer.class.getName());

    public static void main(String[] args) throws IOException, TimeoutException {
        LiftRideConsumer consumer = new LiftRideConsumer();
        consumer.start();
    }

    public void start() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(RABBITMQ_PORT);
        factory.setUsername(RABBITMQ_USERNAME);
        factory.setPassword(RABBITMQ_PASSWORD);

        Connection connection = factory.newConnection();
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS);

        for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicQos(1); // Limit to one message per consumer to ensure fair distribution

            executorService.submit(() -> consumeMessages(channel));
        }
    }

    private void consumeMessages(Channel channel) {
        try {
            channel.basicConsume(QUEUE_NAME, false, (consumerTag, message) -> {
                String msg = new String(message.getBody(), StandardCharsets.UTF_8);
                LiftRide liftRide = gson.fromJson(msg, LiftRide.class);

                // Process message - increment ride count for the skier
                liftRideCounts.merge(liftRide.getSkierID(), 1, Integer::sum);

                // Acknowledge message to RabbitMQ
                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                logger.info("Processed message for skierID: " + liftRide.getSkierID());
            }, consumerTag -> {});
        } catch (IOException e) {
            logger.severe("Error consuming messages: " + e.getMessage());
        }
    }
}
