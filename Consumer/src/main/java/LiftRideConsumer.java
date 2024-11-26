import com.google.gson.Gson;
import com.rabbitmq.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    private static final int NUM_CONSUMER_THREADS = 128; // Adjust thread count based on hardware configuration
    private static final int PREFETCH_COUNT = 50; // Max number of messages pulled by each consumer at once
    private static final int BATCH_SIZE = 50; // Redis Pipeline batch size

    private final Gson gson = new Gson();
    private final Logger logger = Logger.getLogger(LiftRideConsumer.class.getName());
    private static JedisPool jedisPool;

    public static void main(String[] args) throws IOException, TimeoutException {
        // Initialize Redis connection pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(256);  // Max number of Redis connections
        poolConfig.setMaxIdle(128);  // Max idle connections
        poolConfig.setMinIdle(16);   // Min idle connections
        jedisPool = new JedisPool(poolConfig, "44.227.111.231", 6379);

        LiftRideConsumer consumer = new LiftRideConsumer();
        consumer.start();
    }

    public void start() throws IOException, TimeoutException {
        // Setup RabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(RABBITMQ_PORT);
        factory.setUsername(RABBITMQ_USERNAME);
        factory.setPassword(RABBITMQ_PASSWORD);

        // Establish a connection to RabbitMQ
        Connection connection = factory.newConnection();
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS);

        // Create and start multiple consumer threads
        for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicQos(PREFETCH_COUNT); // Set maximum number of messages per consumer

            // Submit the consumer task to executor service
            executorService.submit(() -> consumeMessages(channel));
        }
    }

    /**
     * Method for consuming messages from RabbitMQ.
     * It processes messages in batches and writes to Redis.
     */
    private void consumeMessages(Channel channel) {
        List<DeliveryMessage> batchMessages = new ArrayList<>(); // Store batch messages

        try {
            // Consume messages from the queue
            channel.basicConsume(QUEUE_NAME, false, (consumerTag, message) -> {
                String msg = new String(message.getBody(), StandardCharsets.UTF_8);
                LiftRide liftRide = gson.fromJson(msg, LiftRide.class);

                // Collect messages for batch processing
                batchMessages.add(new DeliveryMessage(message, liftRide));

                // If batch size is reached, process and acknowledge
                if (batchMessages.size() >= BATCH_SIZE) {
                    processBatch(batchMessages, channel);
                    batchMessages.clear(); // Clear batch messages
                }

            }, consumerTag -> {});
        } catch (IOException e) {
            logger.severe("Error consuming messages: " + e.getMessage());
        }
    }

    /**
     * Processes the batch of messages by updating Redis in a batch operation.
     * It also acknowledges the messages in RabbitMQ after processing.
     *
     * @param batchMessages List of messages to be processed.
     * @param channel The RabbitMQ channel used to acknowledge messages.
     */
    private void processBatch(List<DeliveryMessage> batchMessages, Channel channel) {
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined(); // Enable Redis Pipeline batch operations

            // Process each message in the batch
            for (DeliveryMessage deliveryMessage : batchMessages) {
                LiftRide liftRide = deliveryMessage.liftRide;
                String key = "skier:" + liftRide.getSkierID() + ":day:" + liftRide.getDayID();

                // Update Redis hash with lift ride information
                pipeline.hset(key, "resortID", String.valueOf(liftRide.getResortID()));
                pipeline.hset(key, "liftID", String.valueOf(liftRide.getLiftID()));
                pipeline.hincrBy(key, "verticalHeight", liftRide.getLiftID() * 10L); // Increment verticalHeight
            }

            pipeline.sync(); // Execute batch operations

            // Acknowledge the messages in RabbitMQ after processing
            for (DeliveryMessage deliveryMessage : batchMessages) {
                channel.basicAck(deliveryMessage.message.getEnvelope().getDeliveryTag(), false);
            }

            // Log the batch processing
            logger.info("Processed batch of " + batchMessages.size() + " messages.");
        } catch (Exception e) {
            logger.severe("Error processing batch: " + e.getMessage());
        }
    }

    /**
         * Helper class to store RabbitMQ message and corresponding LiftRide object.
         */
        private record DeliveryMessage(Delivery message, LiftRide liftRide) {
    }
}
