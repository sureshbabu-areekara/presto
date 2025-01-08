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
/*
package com.facebook.presto.opentelemetry;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.spi.telemetry.TelemetryTracing;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TelemetryTracingImpl
    implements TelemetryTracing<TelemetryFactory>
{
    private static final Logger log = Logger.get(TelemetryTracingImpl.class);
    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();

    @Override
    public void addOpenTelemetryFactory(TelemetryFactory telemetryFactory)
    {
        requireNonNull(telemetryFactory, "openTelemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(telemetryFactory.getName(), telemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", telemetryFactory.getName()));
        }
    }
}
*/
