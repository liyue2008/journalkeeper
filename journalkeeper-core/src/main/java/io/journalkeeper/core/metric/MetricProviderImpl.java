package io.journalkeeper.core.metric;

import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricSupport;
import io.journalkeeper.utils.actor.Actor;
import io.journalkeeper.utils.spi.ServiceLoadException;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MetricProviderImpl implements MetricProvider {
    private static final Logger logger = LoggerFactory.getLogger(MetricProviderImpl.class);
    private final static JMetric DUMMY_METRIC = new DummyMetric();
    private JMetricFactory metricFactory;
    private Map<String, JMetric> metricMap;
    private boolean isEnableMetric;
    private final Actor actor;

    public MetricProviderImpl(boolean isEnableMetric, int printMetricIntervalSec) {
        this.isEnableMetric = isEnableMetric;
        this.actor = Actor.builder("Metric").setHandlerInstance(this).build();
        if (isEnableMetric) {
            try {
                this.metricFactory = ServiceSupport.load(JMetricFactory.class);
                this.metricMap = new ConcurrentHashMap<>();
                if (printMetricIntervalSec > 0) {
                    actor.addScheduler(printMetricIntervalSec, TimeUnit.SECONDS, "printMetrics", this::printMetrics);
                }
            } catch (ServiceLoadException se) {
                logger.warn("No metric extension found in the classpath, Metric will disabled!");
                this.isEnableMetric = false;
                this.metricFactory = null;
                this.metricMap = null;
            }
        } else {
            this.metricFactory = null;
            this.metricMap = null;
        }
    }

    @Override
    public JMetric getMetric(String name) {
        if (this.isEnableMetric) {
            return metricMap.computeIfAbsent(name, metricFactory::create);
        } else {
            return DUMMY_METRIC;
        }
    }

    @Override
    public boolean isMetricEnabled() {
        return isEnableMetric;
    }

    @Override
    public void removeMetric(String name) {
        if (this.isEnableMetric) {
            metricMap.remove(name);
        }
    }

    public Actor getActor() {
        return actor;
    }

    private void printMetrics() {
        metricMap.values()
                .stream()
                .map(JMetric::getAndReset)
                .map(JMetricSupport::formatNs)
                .forEach(logger::info);
    }
}
