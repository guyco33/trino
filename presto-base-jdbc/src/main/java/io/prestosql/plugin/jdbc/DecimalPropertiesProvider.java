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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.util.List;

import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;

public class DecimalPropertiesProvider
        implements SessionPropertiesProvider
{
    public static final String DECIMAL_MAPPING = "decimal_mapping";
    public static final String DECIMAL_DEFAULT_SCALE = "decimal_default_scale";
    public static final String DECIMAL_ROUNDING_MODE = "decimal_rounding_mode";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public DecimalPropertiesProvider(DecimalConfig decimalConfig)
    {
        properties = ImmutableList.of(
                enumProperty(
                        DECIMAL_MAPPING,
                        "Decimal mapping for exceeding precision and unspecified scale",
                        DecimalMapping.class,
                        decimalConfig.getDecimalMapping(),
                        false),
                integerProperty(
                        DECIMAL_DEFAULT_SCALE,
                        "Default decimal scale for decimals with unspecified scale",
                        decimalConfig.getDecimalDefaultScale(),
                        false),
                enumProperty(
                        DECIMAL_ROUNDING_MODE,
                        "Rounding mode for decimals with unspecified scale. Rounding is not used when decimal-mapping is set to strict. Rounding is disabled when is set to UNNECESSARY",
                        RoundingMode.class,
                        decimalConfig.getDecimalRoundingMode(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static DecimalMapping getDecimalRounding(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_MAPPING, DecimalMapping.class);
    }

    public static int getDecimalDefaultScale(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_DEFAULT_SCALE, Integer.class);
    }

    public static RoundingMode getDecimalRoundingMode(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_ROUNDING_MODE, RoundingMode.class);
    }
}
