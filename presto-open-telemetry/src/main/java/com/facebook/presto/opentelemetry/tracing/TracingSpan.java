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
package com.facebook.presto.opentelemetry.tracing;

import com.facebook.presto.spi.telemetry.BaseSpan;
import io.opentelemetry.api.trace.Span;

public class TracingSpan
        implements BaseSpan
{
    private final Span span;

    public TracingSpan(Span span)
    {
        this.span = span;
    }

    public Span getSpan()
    {
        return span;
    }

    public static TracingSpan getInvalid()
    {
        return new TracingSpan(Span.getInvalid());
    }

    public static TracingSpan current()
    {
        return new TracingSpan(Span.current());
    }

    public TracingSpan setAttribute(String key, String value)
    {
        return new TracingSpan(span.setAttribute(key, value));
    }

    public TracingSpan setAttribute(String key, long value)
    {
        return new TracingSpan(span.setAttribute(key, value));
    }

    public boolean isRecording()
    {
        return span.isRecording();
    }

    @Override
    public void end()
    {
        span.end();
    }
}
