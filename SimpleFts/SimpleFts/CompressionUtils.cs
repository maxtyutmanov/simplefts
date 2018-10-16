using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public static class CompressionUtils
    {
        public static async Task CompressChunkAndAppendTo(this Stream source, Stream target)
        {
            source.Position = 0;

            await target.WriteLong(source.Length);

            GZipStream gzip = null;
            try
            {
                gzip = new GZipStream(target, CompressionMode.Compress, true);
                await source.CopyToAsync(gzip);
            }
            finally
            {
                gzip?.Dispose();
            }
        }

        public static async Task<byte[]> GetDecompressedChunk(this Stream stream, long chunkOffset)
        {
            stream.Position = chunkOffset;
            long originalLenght = await stream.ReadLong();

            using (var gzip = new GZipStream(stream, CompressionMode.Decompress, true))
            {
                var buffer = new byte[originalLenght];
                await gzip.ReadAsync(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        private static async Task<long> ReadLong(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            await stream.ReadAsync(buffer, 0, buffer.Length);

            return BitConverter.ToInt64(buffer, 0);
        }

        private static async Task WriteLong(this Stream stream, long val)
        {
            var bytes = BitConverter.GetBytes(val);
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }
    }
}
