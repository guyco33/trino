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
package io.prestosql.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.math.RoundingMode;

import static java.math.RoundingMode.UNNECESSARY;

public class DecimalConfig
{
    private DecimalMapping decimalMapping = DecimalMapping.STRICT;
    private int decimalDefaultScale = 16;
    private RoundingMode decimalRoundingMode = UNNECESSARY;

    public enum DecimalMapping {
        STRICT,
        ALLOW_OVERFLOW,
        /**/;
    }

    public DecimalMapping getDecimalMapping()
    {
        return decimalMapping;
    }

    @Config("decimal-mapping")
    @ConfigDescription("Decimal mapping for exceeding precision and unspecified scale")
    public DecimalConfig setDecimalMapping(DecimalMapping decimalMapping)
    {
        this.decimalMapping = decimalMapping;
        return this;
    }

    @Min(0)
    @Max(38)
    public Integer getDecimalDefaultScale()
    {
        return decimalDefaultScale;
    }

    @Config("decimal-default-scale")
    @ConfigDescription("Default decimal scale for decimals with unspecified scale")
    public DecimalConfig setDecimalDefaultScale(Integer decimalDefaultScale)
    {
        this.decimalDefaultScale = decimalDefaultScale;
        return this;
    }

    @NotNull
    public RoundingMode getDecimalRoundingMode()
    {
        return decimalRoundingMode;
    }

    @Config("decimal-rounding-mode")
    @ConfigDescription("Rounding mode for decimals with unspecified scale. Rounding is not used when decimal-mapping is set to strict. Rounding is disabled when is set to UNNECESSARY")
    public DecimalConfig setDecimalRoundingMode(RoundingMode decimalRoundingMode)
    {
        this.decimalRoundingMode = decimalRoundingMode;
        return this;
    }
}
