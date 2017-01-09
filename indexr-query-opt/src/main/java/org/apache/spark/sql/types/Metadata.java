/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.util.JsonUtil;

/**
 * :: DeveloperApi ::
 * 
 * Metadata is a wrapper over Map[STRING, Any] that limits the value type to simple ones: Boolean,
 * LONG, DOUBLE, STRING, Metadata, Array[Boolean], Array[LONG], Array[DOUBLE], Array[STRING], and
 * Array[Metadata]. JSON is used for serialization.
 * 
 * The default constructor is private. User should use either [[MetadataBuilder]] or
 * [[Metadata.fromJson()]] to create Metadata instances.
 *
 */
public class Metadata {
    @JsonUnwrapped
    public Map<String, Object> map;

    @JsonCreator
    public Metadata(Map<String, Object> map) {
        this.map = map;
    }

    public Metadata() {
        this(new HashMap<>());
    }

    /** Tests whether this Metadata contains a binding for a key. */
    public boolean contains(String key) {
        return map.containsKey(key);
    }

    /** Gets a LONG. */
    public Long getLong(String key) {
        return get(key);
    }

    /** Gets a DOUBLE. */
    public Double getDouble(String key) {
        return get(key);
    }

    /** Gets a Boolean. */
    public Boolean getBoolean(String key) {
        return get(key);
    }

    /** Gets a STRING. */
    public String getString(String key) {
        return get(key);
    }

    /** Gets a Metadata. */
    public Metadata getMetadata(String key) {
        return get(key);
    }

    /** Gets a LONG array. */
    public List<Long> getLongArray(String key) {
        return get(key);
    }

    /** Gets a DOUBLE array. */
    public List<Double> getDoubleArray(String key) {
        return get(key);
    }

    /** Gets a Boolean array. */
    public List<Boolean> getBooleanArray(String key) {
        return get(key);
    }

    /** Gets a STRING array. */
    public List<String> getStringArray(String key) {
        return get(key);
    }

    /** Gets a Metadata array. */
    public List<Metadata> getMetadataArray(String key) {
        return get(key);
    }


    public String json() {
        return JsonUtil.toJson(this);
    }

    @Override
    public String toString() {
        return json();
    }

    private <T> T get(String key) {
        return (T) map.get(key);
    }


    /** Puts a LONG. */
    public void putLong(String key, Long value) {
        put(key, value);
    }

    /** Puts a DOUBLE. */
    public void putDouble(String key, Double value) {
        put(key, value);
    }

    /** Puts a Boolean. */
    public void putBoolean(String key, Boolean value) {
        put(key, value);
    }

    /** Puts a STRING. */
    public void putString(String key, String value) {
        put(key, value);
    }

    /** Puts a [[Metadata]]. */
    public void putMetadata(String key, Metadata value) {
        put(key, value);
    }

    /** Puts a LONG array. */
    public void putLongArray(String key, List<Long> value) {
        put(key, value);
    }

    /** Puts a DOUBLE array. */
    public void putDoubleArray(String key, List<Double> value) {
        put(key, value);
    }

    /** Puts a Boolean array. */
    public void putBooleanArray(String key, List<Boolean> value) {
        put(key, value);
    }

    /** Puts a STRING array. */
    public void putStringArray(String key, List<String> value) {
        put(key, value);
    }

    /** Puts a [[Metadata]] array. */
    public void putMetadataArray(String key, List<Metadata> value) {
        put(key, value);
    }

    private void put(String key, Object value) {
        map.put(key, value);
    }

    public static Metadata fromJson(String json) {
        return JsonUtil.fromJson(json, Metadata.class);
    }

    public static Metadata empty = new Metadata(Collections.emptyMap());

}

