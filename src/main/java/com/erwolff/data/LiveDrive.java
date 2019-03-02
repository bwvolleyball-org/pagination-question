package com.erwolff.data;

/**
 * Simple class representing an "ongoing" drive
 */
public class LiveDrive {

    private DriveType type;
    private long timestamp;

    public LiveDrive() {
    }

    public LiveDrive(long timestamp) {
        this.type = DriveType.LIVE;
        this.timestamp = timestamp;
    }

    public LiveDrive(DriveType type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public DriveType getType() {
        return type;
    }

    public void setType(DriveType type) {
        this.type = type;
    }
}
