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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;

/**
 * Allow access to certain package private methods of WriterOptions,
 * primarily used for providing the memory manager to the writer
 */
public class OrcWriterOptions
        extends OrcFile.WriterOptions
{
    public OrcWriterOptions(Configuration conf)
    {
        super(conf);
    }

    @Override
    public OrcWriterOptions memory(MemoryManager value)
    {
        super.memory(value);
        return this;
    }
}
