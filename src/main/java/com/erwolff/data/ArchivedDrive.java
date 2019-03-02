package com.erwolff.data;

/**
 * Simple class representing a drive that has ended and been archived
 */
public class ArchivedDrive {

    private DriveType type;
    private long timestamp;

    public ArchivedDrive() {
    }

    public ArchivedDrive(long timestamp) {
        this.type = DriveType.ARCHIVED;
        this.timestamp = timestamp;
    }

    public ArchivedDrive(DriveType type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(DriveType type) {
        this.type = type;
    }

    public DriveType getType() {
        return type;
    }
}
