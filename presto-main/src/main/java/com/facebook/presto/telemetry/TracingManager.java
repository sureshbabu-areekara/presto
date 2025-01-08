/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.telemetry;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.google.errorprone.annotations.MustBeClosed;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetryManager class creates and manages OpenTelemetry and Tracer instances.
 */
public class TracingManager
{
    private static final Logger log = Logger.get(TracingManager.class);
    private static final File OPENTELEMETRY_CONFIGURATION = new File("etc/telemetry-tracing.properties");
    private static final String TRACING_FACTORY_NAME = "tracing-factory.name";

    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private static TelemetryFactory telemetryFactory;

    public TracingManager()
    {
        //addOpenTelemetryFactory(new OpenTelemetryImpl());
    }

    /**
     * adds and registers all the OpenTelemetryFactory implementations to support different configurations
     * @param openTelemetryFactory
     */
    public void addOpenTelemetryFactory(TelemetryFactory openTelemetryFactory)
    {
        requireNonNull(openTelemetryFactory, "openTelemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(openTelemetryFactory.getName(), openTelemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", openTelemetryFactory.getName()));
        }
    }

    public void clearFactories()
    {
        openTelemetryFactories.clear();
    }

    /**
     * called from PrestoServer for loading the properties after OpenTelemetryManager is bound and injected
     * @throws Exception
     */
    public void loadConfiguredOpenTelemetry()
            throws Exception
    {
        if (OPENTELEMETRY_CONFIGURATION.exists()) {
            Map<String, String> properties = loadProperties(OPENTELEMETRY_CONFIGURATION);
            checkArgument(
                    !isNullOrEmpty(properties.get(TRACING_FACTORY_NAME)),
                    "Opentelemetry configuration %s does not contain %s",
                    OPENTELEMETRY_CONFIGURATION.getAbsoluteFile(),
                    TRACING_FACTORY_NAME);

            if (properties.isEmpty()) {
                log.debug("telemetry properties not loaded");
            }

            properties = new HashMap<>(properties);
            String openTelemetryFactoryName = properties.remove(TRACING_FACTORY_NAME);

            checkArgument(!isNullOrEmpty(openTelemetryFactoryName), "otel-factory.name property must be present");

            TelemetryFactory openTelemetryFactory = openTelemetryFactories.get(openTelemetryFactoryName);
            checkState(openTelemetryFactory != null, "Opentelemetry factory %s is not registered", openTelemetryFactoryName);
            this.telemetryFactory = openTelemetryFactory;

            log.debug("setting telemetry properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
            openTelemetryFactory.loadConfiguredOpenTelemetry();
        }
    }

/*    public static Tracer getTracer()
    {
        return tracer;
    }

    public static void setTracer(Tracer tracer)
    {
        TracingManager.tracer = tracer;
    }

    public OpenTelemetry getOpenTelemetry()
    {
        return this.configuredOpenTelemetry;
    }

    public static void setOpenTelemetry(OpenTelemetry configuredOpenTelemetry)
    {
        TracingManager.configuredOpenTelemetry = configuredOpenTelemetry;
    }*/

    private static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    /*public static Runnable getCurrentContextWrap(Runnable runnable)
    {
        return telemetryFactory.getCurrentContextWrap(runnable);
    }

    private static Context getCurrentContext()
    {
        return Context.current();
    }

    private static Context getCurrentContextWith(TracingSpan tracingSpan)
    {
        return Context.current().with(tracingSpan.getSpan());
    }

    private static Context getContext(TracingSpan span)
    {
        return span != null ? getCurrentContextWith(span) : getCurrentContext();
    }*/

/*    private static Context getContext(String traceParent)
    {
        TextMapPropagator propagator = configuredOpenTelemetry.getPropagators().getTextMapPropagator();
        Context context = propagator.extract(Context.current(), traceParent, new TextMapGetterImpl());
        return context;
    }*/

    public static boolean isRecording()
    {
        return telemetryFactory.isRecording();
    }

    public static Map<String, String> getHeadersMap(Object span)
    {
        return telemetryFactory.getHeadersMap(span);
    }

    public static void endSpanOnError(Object querySpan, Throwable throwable)
    {
        telemetryFactory.endSpanOnError(querySpan, throwable);
    }

    public static void addEvent(String eventState, Object querySpan)
    {
        telemetryFactory.addEvent(eventState, querySpan);
    }

    public static void setAttributeQueryType(Object querySpan, String queryType)
    {
        telemetryFactory.setAttributeQueryType(querySpan, queryType);
    }

    public static void recordException(Object querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        telemetryFactory.recordException(querySpan, message, runtimeException, errorCode);
    }

    public static void setSuccess(Object querySpan)
    {
        telemetryFactory.setSuccess(querySpan);
    }

    //GetSpans
    public static Object getRootSpan()
    {
        return telemetryFactory.getRootSpan();
    }

    public static Object getSpan(String spanName)
    {
        return telemetryFactory.getSpan(spanName);
    }

    public static Object getSpan(String traceParent, String spanName)
    {
        return telemetryFactory.getSpan(traceParent, spanName);
    }

    public static Object getSpan(Object parentSpan, String spanName, Map<String, String> attributes)
    {
        return telemetryFactory.getSpan(parentSpan, spanName, attributes);
    }

    public static Optional<String> spanString(Object span)
    {
        return telemetryFactory.spanString(span);
    }

    //Scoped Span
    /**
     * starts a basic span and passes it to overloaded method. This method is used for creating basic spans with no attributes.
     * @param name name of span to be created
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    public static AutoCloseable scopedSpan(String name, Boolean... skipSpan)
    {
        return (AutoCloseable) telemetryFactory.scopedSpan(name, skipSpan);
    }

    /**
     * creates a ScopedSpan with the current span. This method is used when we manually create spans in the classes and
     * set attributes to them before passing to the Scopedspan.
     * @param span created span instance
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    public static AutoCloseable scopedSpan(Object span, Boolean... skipSpan)
    {
        return (AutoCloseable) telemetryFactory.scopedSpan(span, skipSpan);
    }

    @MustBeClosed
    public static AutoCloseable scopedSpan(Object parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return (AutoCloseable) telemetryFactory.scopedSpan(parentSpan, spanName, attributes, skipSpan);
    }

    @MustBeClosed
    public static AutoCloseable scopedSpan(Object parentSpan, String spanName, Boolean... skipSpan)
    {
        return (AutoCloseable) telemetryFactory.scopedSpan(parentSpan, spanName, skipSpan);
    }

    @MustBeClosed
    public static AutoCloseable scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return (AutoCloseable) telemetryFactory.scopedSpan(spanName, attributes, skipSpan);
    }
}
