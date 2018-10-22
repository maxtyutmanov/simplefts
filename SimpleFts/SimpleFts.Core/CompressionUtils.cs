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

            await target.WriteLongAsync(source.Length);

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
            long originalLength = await stream.ReadLongAsync();

            using (var gzip = new GZipStream(stream, CompressionMode.Decompress, true))
            {
                var buffer = new byte[originalLength];
                var bytesRead = 0;
                while (bytesRead < originalLength)
                {
                    var bytesReadThisTime = await gzip.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
                    bytesRead += bytesReadThisTime;
                }
                
                return buffer;
            }
        }
    }
}
