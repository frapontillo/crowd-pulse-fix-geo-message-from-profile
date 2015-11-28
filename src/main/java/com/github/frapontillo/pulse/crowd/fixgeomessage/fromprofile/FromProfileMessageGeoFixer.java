/*
 * Copyright 2015 Francesco Pontillo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.frapontillo.pulse.crowd.fixgeomessage.fromprofile;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Profile;
import com.github.frapontillo.pulse.crowd.data.plugin.GenericDbConfig;
import com.github.frapontillo.pulse.crowd.data.repository.ProfileRepository;
import com.github.frapontillo.pulse.crowd.fixgeomessage.IMessageGeoFixerOperator;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.google.gson.JsonElement;
import rx.Observable;

/**
 * Implementation of an {@link IPlugin} that, relying on {@link IMessageGeoFixerOperator}, fixes
 * the geo-location of streamed {@link Message}s using the geo-location information in the author's
 * {@link Profile}, if any.
 *
 * @author Francesco Pontillo
 */
public class FromProfileMessageGeoFixer extends
        IPlugin<Message, Message, FromProfileMessageGeoFixer.FromProfileMessageGeoFixerOptions> {
    public final static String PLUGIN_NAME = "fromprofile";
    private ProfileRepository profileRepository;

    @Override public String getName() {
        return PLUGIN_NAME;
    }

    @Override public FromProfileMessageGeoFixerOptions getNewParameter() {
        return new FromProfileMessageGeoFixerOptions();
    }

    @Override public Observable.Operator<Message, Message> getOperator(
            FromProfileMessageGeoFixerOptions parameters) {
        // use the custom DB name, if any
        profileRepository = new ProfileRepository(parameters.getDb());

        return new IMessageGeoFixerOperator(this) {
            @Override public Double[] getCoordinates(Message message) {
                Profile user = profileRepository.getByUsername(message.getFromUser());
                Double[] coordinates = null;
                if (user != null && user.getLatitude() != null && user.getLongitude() != null) {
                    coordinates = new Double[]{user.getLatitude(), user.getLongitude()};
                }
                return coordinates;
            }
        };
    }

    /**
     * Geo fixing configuration based on the database configuration from {@link GenericDbConfig}.
     */
    public class FromProfileMessageGeoFixerOptions
            extends GenericDbConfig<FromProfileMessageGeoFixerOptions> {
        @Override public FromProfileMessageGeoFixerOptions buildFromJsonElement(JsonElement json) {
            return PluginConfigHelper.buildFromJson(json, FromProfileMessageGeoFixerOptions.class);
        }
    }
}
