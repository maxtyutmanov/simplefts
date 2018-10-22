using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class CompressionUtils
    {
        private byte[] _readBuffer = new byte[100 * 1024];
        private byte[] _originalLengthBuffer = new byte[sizeof(long)];

        public async Task CopyWithCompression(Stream source, Stream target)
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

        public async Task<ArraySegment<byte>> ReadWithDecompression(Stream source, long chunkOffset)
        {
            source.Position = chunkOffset;
            long originalLength = await source.ReadLongAsync();

            if (originalLength > _readBuffer.Length)
            {
                // grow buffer
                _readBuffer = new byte[originalLength];
            }

            using (var gzip = new GZipStream(source, CompressionMode.Decompress, true))
            {
                var bytesRead = 0;
                while (bytesRead < originalLength)
                {
                    var bytesReadThisTime = await gzip.ReadAsync(_readBuffer, bytesRead, (int)originalLength - bytesRead);
                    bytesRead += bytesReadThisTime;
                }
                
                return new ArraySegment<byte>(_readBuffer, 0, (int)originalLength);
            }
        }
    }
}
