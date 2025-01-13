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

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.spi.telemetry.TelemetryTracing;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

/*import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;*/

public class OpenTelemetryTracing
        implements TelemetryTracing
{
    public static final String NAME = "otel";

    private static final OpenTelemetryTracing INSTANCE = new OpenTelemetryTracing();

    public static class Factory
            implements TelemetryFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public TelemetryTracing create()
        {
            //requireNonNull(config, "config is null");
            //checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void loadConfiguredOpenTelemetry()
    {
        return;
    }

    @Override
    public Runnable getCurrentContextWrap(Runnable runnable)
    {
        return null;
    }

    @Override
    public boolean isRecording()
    {
        return false;
    }

    @Override
    public Map<String, String> getHeadersMap(Object span)
    {
        return ImmutableMap.of();
    }

    @Override
    public void endSpan(Object span)
    {
        return;
    }

    @Override
    public void endSpanOnError(Object querySpan, Throwable throwable)
    {
        return;
    }

    @Override
    public void addEvent(Object span, String eventName)
    {
        return;
    }

    @Override
    public void addEvent(Object querySpan, String eventName, String eventState)
    {
        return;
    }

    @Override
    public void setAttributes(Object span, Map attributes)
    {
        return;
    }

    @Override
    public void recordException(Object querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        return;
    }

    @Override
    public void setSuccess(Object querySpan)
    {
        return;
    }

    @Override
    public Object getRootSpan()
    {
        return null;
    }

    @Override
    public Object getSpan(String spanName)
    {
        return null;
    }

    @Override
    public Object getSpan(String traceParent, String spanName)
    {
        return null;
    }

    @Override
    public Optional<String> spanString(Object span)
    {
        return Optional.empty();
    }

    @Override
    public Object scopedSpan(String name, Boolean... skipSpan)
    {
        return null;
    }

    @Override
    public Object scopedSpan(Object span, Boolean... skipSpan)
    {
        return null;
    }

    @Override
    public Object scopedSpan(Object parentSpan, String spanName, Boolean... skipSpan)
    {
        return null;
    }

    @Override
    public Object scopedSpan(String spanName, Map attributes, Boolean... skipSpan)
    {
        return null;
    }

    @Override
    public Object scopedSpan(Object parentSpan, String spanName, Map attributes, Boolean... skipSpan)
    {
        return null;
    }

    @Override
    public Object getSpan(Object parentSpan, String spanName, Map attributes)
    {
        return null;
    }
}
