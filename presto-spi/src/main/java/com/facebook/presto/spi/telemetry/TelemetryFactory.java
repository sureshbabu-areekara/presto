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
package com.facebook.presto.spi.telemetry;

import com.facebook.presto.common.ErrorCode;
import com.google.errorprone.annotations.MustBeClosed;

import java.util.Map;
import java.util.Optional;

public interface TelemetryFactory<T, U>
{
    String getName();

    void loadConfiguredOpenTelemetry();

    Runnable getCurrentContextWrap(Runnable runnable);

    boolean isRecording();

    Map<String, String> getHeadersMap(T span);

    void endSpanOnError(T querySpan, Throwable throwable);

    void addEvent(String eventState, T querySpan);

    void setAttributeQueryType(T querySpan, String queryType);

    void recordException(T querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode);

    void setSuccess(T querySpan);

    //GetSpans
    T getRootSpan();

    T getSpan(String spanName);

    T getSpan(String traceParent, String spanName);

    T getSpan(T parentSpan, String spanName, Map<String, String> attributes);

    Optional<String> spanString(T span);

    //scoped spans
    @MustBeClosed
    U scopedSpan(String name, Boolean... skipSpan);

    @MustBeClosed
    U scopedSpan(T span, Boolean... skipSpan);

    @MustBeClosed
    U scopedSpan(T parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan);

    @MustBeClosed
    U scopedSpan(T parentSpan, String spanName, Boolean... skipSpan);

    @MustBeClosed
    U scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan);
}
