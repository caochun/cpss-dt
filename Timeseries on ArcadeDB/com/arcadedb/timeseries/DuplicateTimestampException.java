package com.arcadedb.timeseries;

public class DuplicateTimestampException extends TimeseriesException{

    public DuplicateTimestampException(String message) {
        super(message);
    }
}
