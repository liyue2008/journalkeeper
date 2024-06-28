package io.journalkeeper.core.resilience;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 限制在途请求数量的限流器
 */
public class InFlightRequestRateLimiter{

    private final int maxInFlightRequest;
    private final AtomicInteger inFlightRequest = new AtomicInteger(0);

    public InFlightRequestRateLimiter(int maxInFlightRequest) {
        this.maxInFlightRequest = maxInFlightRequest;
    }
    public void acquire() throws InterruptedException {
        while (inFlightRequest.get() + 1 >= maxInFlightRequest) {
            synchronized (this) {
                wait(1L);
            }
        }
        inFlightRequest.getAndIncrement();
    }

    public void release() {
        if (inFlightRequest.getAndDecrement() >= maxInFlightRequest) {
            synchronized (this) {
                notifyAll();
            }
        }
    }
}
