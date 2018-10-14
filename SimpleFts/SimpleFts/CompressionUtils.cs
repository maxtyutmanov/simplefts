using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace SimpleFts
{
    public static class CompressionUtils
    {
        public static void CompressAndAppendTo(this Stream source, Stream target)
        {
            source.Position = 0;

            var chunkHeaderPosition = target.Position;
            target.Position += sizeof(long);

            var targetStreamPrevPosition = target.Position;
            GZipStream gzip = null;
            try
            {
                gzip = new GZipStream(target, CompressionMode.Compress, true);
                source.CopyTo(gzip);
                gzip.Close();

                // prepend the chunk with its length in the compressed stream
                var compressedLength = target.Position - targetStreamPrevPosition;
                target.Position = chunkHeaderPosition;
                target.WriteLong(compressedLength);
            }
            finally
            {
                gzip?.Dispose();
            }
        }

        public static byte[] DecompressChunk(this Stream stream, long chunkOffset)
        {
            stream.Position = chunkOffset;
            long compressedLength = stream.ReadLong();
            
            using (var gzip = new GZipStream(stream, CompressionMode.Decompress, true))
            {
                var buffer = new byte[compressedLength];
                gzip.Read(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        private static long ReadLong(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            stream.Read(buffer, 0, buffer.Length);

            return BitConverter.ToInt64(buffer, 0);
        }

        private static void WriteLong(this Stream stream, long val)
        {
            var bytes = BitConverter.GetBytes(val);
            stream.Write(bytes, 0, bytes.Length);
        }
    }
}
