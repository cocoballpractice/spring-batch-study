package cocoball.springbatchstudy.part6;

import java.util.Objects;

public enum Level {

    VIP(500_000, null),
    GOLD(500_000, VIP),
    SILVER(300_000, GOLD),
    NORMAL(200_000, SILVER);

    private final int nextAmout;
    private final Level nextLevel;

    Level(int nextAmount, Level nextLevel) {
        this.nextAmout = nextAmount;
        this.nextLevel = nextLevel;
    }

    public static boolean availableLevelUp(Level level, int totalAmount) {

        if (Objects.isNull(level)) {
            return false;
        }

        if (Objects.isNull(level.nextLevel)) {
            return false;
        }

        return totalAmount >= level.nextAmout;
    }

    public static Level getNextLevel(int totalAmount) {

        if (totalAmount >= Level.VIP.nextAmout) {
            return VIP;
        }

        if (totalAmount >= Level.GOLD.nextAmout) {
            return GOLD.nextLevel;
        }

        if (totalAmount >= Level.SILVER.nextAmout) {
            return SILVER.nextLevel;
        }

        if (totalAmount >= Level.NORMAL.nextAmout) {
            return NORMAL.nextLevel;
        }

        return NORMAL;

    }

}
