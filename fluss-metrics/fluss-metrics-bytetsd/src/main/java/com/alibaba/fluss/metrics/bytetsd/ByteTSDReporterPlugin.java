package com.alibaba.fluss.metrics.bytetsd;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.reporter.MetricReporter;
import com.alibaba.fluss.metrics.reporter.MetricReporterPlugin;

public class ByteTSDReporterPlugin implements MetricReporterPlugin {

    private static final String PLUGIN_NAME = "bytetsd";

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        return new ByteTSDReporter();
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }
}
