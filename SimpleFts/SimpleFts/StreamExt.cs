using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public static class StreamExt
    {
        public static async Task WriteInt(this Stream stream, int val)
        {
            var buffer = BitConverter.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        public static async Task WriteLong(this Stream stream, long val)
        {
            var buffer = BitConverter.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        public static async Task<long> ReadLong(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            await stream.ReadAsync(buffer, 0, buffer.Length);

            return BitConverter.ToInt64(buffer, 0);
        }

        public static async Task WriteString(this Stream stream, string val)
        {
            var buffer = Encoding.UTF8.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }
    }
}
