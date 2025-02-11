package com.alibaba.fluss.metrics.bytetsd;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.metrics.reporter.ScheduledMetricReporter;

import com.bytedance.metrics.simple.SimpleByteTSDMetrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ByteTSDReporter implements ScheduledMetricReporter {

    private final List<Metric> metrics = new ArrayList<>(1024);
    private final List<String> metricNames = new ArrayList<>(1024);
    private final List<String> metricTags = new ArrayList<>(1024);

    private SimpleByteTSDMetrics metricsClient;

    @Override
    public void open(Configuration config) {
        metricsClient = SimpleByteTSDMetrics.builder().prefix("data.streaming.fluss").build();
    }

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        HashMap<String, String> tags = new HashMap<>(group.getAllVariables().size());
        String finalMetricName =
                group.getLogicalScope(CharacterFilter.NO_OP_FILTER, '.') + "." + metricName;

        for (Map.Entry<String, String> entry : group.getAllVariables().entrySet()) {
            if (entry.getValue() == null || "".equals(entry.getValue())) {
                continue;
            }
            tags.put(entry.getKey(), entry.getValue());
        }

        metricTags.add(tags.toString());
        metricNames.add(finalMetricName);
        metrics.add(metric);

        System.out.println("registering " + finalMetricName + ", with tags: " + tags);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {}

    @Override
    public void report() {
        for (int i = 0; i < metrics.size(); i++) {
            report(metrics.get(i), metricNames.get(i), metricTags.get(i));
        }
    }

    private void report(Metric metric, String metricName, String tags) {
        switch (metric.getMetricType()) {
            case COUNTER:
                Counter counter = (Counter) metric;
                counter.getCount();
                metricsClient.emitStore(metricName, counter.getCount(), tags);
                System.out.println(
                        "reporting counter "
                                + metricName
                                + ", with tags: "
                                + tags
                                + ", value: "
                                + counter.getCount());
                break;
            case GAUGE:
                Gauge gauge = (Gauge) metric;
                metricsClient.emitStore(metricName, gaugeValue(gauge), tags);
                System.out.println(
                        "reporting gauge "
                                + metricName
                                + ", with tags: "
                                + tags
                                + ", value: "
                                + gauge.getValue());
                break;
            case METER:
                Meter meter = (Meter) metric;
                meter.getCount();
                meter.getRate();
                metricsClient.emitStore(metricName, meter.getRate(), tags);
                System.out.println(
                        "reporting meter "
                                + metricName
                                + ", with tags: "
                                + tags
                                + ", value: "
                                + meter.getCount());
                break;
            case HISTOGRAM:
                Histogram histogram = (Histogram) metric;
                histogram.getStatistics().getQuantile(0.9);
                metricsClient.emitStore(
                        metricName + ".90", histogram.getStatistics().getQuantile(0.90), tags);
                metricsClient.emitStore(
                        metricName + ".95", histogram.getStatistics().getQuantile(0.95), tags);
                metricsClient.emitStore(
                        metricName + ".99", histogram.getStatistics().getQuantile(0.99), tags);
                System.out.println(
                        "reporting histogram "
                                + metricName
                                + ", with tags: "
                                + tags
                                + ", value: "
                                + histogram.getStatistics().getQuantile(0.9));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown metric type: " + metric.getMetricType());
        }
    }

    double gaugeValue(Gauge<?> gauge) {
        final Object value = gauge.getValue();
        if (value == null) {
            //            LOG.debug("Gauge {} is null-valued, defaulting to 0.", gauge);
            return 0;
        }
        if (value instanceof Double) {
            return (double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        //        LOG.debug(
        //                "Invalid type for Gauge {}: {}, only number types and booleans are
        // supported by this reporter.",
        //                gauge,
        //                value.getClass().getName());
        return 0;
    }

    @Override
    public Duration scheduleInterval() {
        return Duration.ofSeconds(30);
    }
}
