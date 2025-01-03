import java.util.concurrent.ThreadLocalRandom;

public class LiftRideGenerator {
    public static LiftRide generateLiftRide() {
        int skierID = ThreadLocalRandom.current().nextInt(1, 100001);
        int resortID = ThreadLocalRandom.current().nextInt(1, 11);
        int liftID = ThreadLocalRandom.current().nextInt(1, 41);
        String seasonID = "2024";
        String dayID = "1";
        int time = ThreadLocalRandom.current().nextInt(1, 361);

        return new LiftRide(skierID, resortID, liftID, seasonID, dayID, time);
    }
}

