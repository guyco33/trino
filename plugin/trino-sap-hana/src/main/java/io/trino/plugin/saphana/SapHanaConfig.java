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
package io.trino.plugin.saphana;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class SapHanaConfig
{
    private boolean synonymsEnabled;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(60, TimeUnit.SECONDS);

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("sap_hana.max-reconnects")
    public SapHanaConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("sap_hana.connection-timeout")
    public SapHanaConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Config("sap_hana.synonyms.enabled")
    public SapHanaConfig setSynonymsEnabled(boolean enabled)
    {
        this.synonymsEnabled = enabled;
        return this;
    }

    @NotNull
    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }
}
