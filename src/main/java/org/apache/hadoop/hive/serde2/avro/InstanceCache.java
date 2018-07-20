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
package org.apache.hadoop.hive.serde2.avro;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;

import java.util.Set;

/**
 * This is a thread-safe, size-bounded fork of the Hive version.
 * It also includes the correctness fix from HIVE-11288.
 */
public abstract class InstanceCache<K, V>
{
    private final Cache<K, V> cache = Caffeine.newBuilder()
            .maximumSize(100_000)
            .build();

    protected InstanceCache() {}

    public V retrieve(K hv)
            throws AvroSerdeException
    {
        return retrieve(hv, null);
    }

    public V retrieve(K hv, Set<K> seenSchemas)
            throws AvroSerdeException
    {
        V instance = cache.getIfPresent(hv);

        if (instance == null) {
            instance = makeInstance(hv, seenSchemas);
            cache.put(hv, instance);
        }

        return instance;
    }

    protected abstract V makeInstance(K hv, Set<K> seenSchemas)
            throws AvroSerdeException;
}
