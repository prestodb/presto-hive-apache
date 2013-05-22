package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Codec reuse is broken, so disable pooling by overriding the original class.
 */
@SuppressWarnings("UnusedDeclaration")
public final class CodecPool
{
    private CodecPool() {}

    public static Compressor getCompressor(CompressionCodec codec)
    {
        return codec.createCompressor();
    }

    public static Decompressor getDecompressor(CompressionCodec codec)
    {
        return codec.createDecompressor();
    }

    public static void returnCompressor(Compressor compressor)
    {
        // no-op
    }

    public static void returnDecompressor(Decompressor decompressor)
    {
        // no-op
    }
}
