package org.mulla.in.order.service.process;

public class KafkaSendFailedException extends RuntimeException {
    public KafkaSendFailedException(String s, Exception e) {
        super(s, e);
    }
}
