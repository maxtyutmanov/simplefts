using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace SimpleFts
{
    public static class CompressionUtils
    {
        public static void CompressChunkAndAppendTo(this Stream source, Stream target)
        {
            source.Position = 0;

            target.WriteLong(source.Length);

            GZipStream gzip = null;
            try
            {
                gzip = new GZipStream(target, CompressionMode.Compress, true);
                source.CopyTo(gzip);
            }
            finally
            {
                gzip?.Dispose();
            }
        }

        public static byte[] DecompressChunk(this Stream stream, long chunkOffset)
        {
            stream.Position = chunkOffset;
            long originalLenght = stream.ReadLong();

            using (var gzip = new GZipStream(stream, CompressionMode.Decompress, true))
            {
                var buffer = new byte[originalLenght];
                gzip.Read(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        private static int ReadInt(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            stream.Read(buffer, 0, buffer.Length);

            return BitConverter.ToInt32(buffer, 0);
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
