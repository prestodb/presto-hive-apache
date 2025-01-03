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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Custom fork of org.apache.hadoop.hive.serde2.io.DoubleWritable
 */
public class DoubleWritable extends org.apache.hadoop.io.DoubleWritable {

    public DoubleWritable() {
        super();
    }

    public DoubleWritable(double value) {
        super(value);
    }

    static { // register this comparator
        WritableComparator.define(DoubleWritable.class, new Comparator());
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(get());
        return (int) (bits ^ (bits >>> 32));
    }
}
