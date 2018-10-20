using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public static class StreamExt
    {
        public static async Task WriteIntAsync(this Stream stream, int val)
        {
            var buffer = BitConverter.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        public static async Task<int> ReadIntAsync(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            await stream.ReadAsync(buffer, 0, buffer.Length);
            return BitConverter.ToInt32(buffer, 0);
        }

        public static int ReadInt(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            stream.Read(buffer, 0, buffer.Length);
            return BitConverter.ToInt32(buffer, 0);
        }

        public static async Task WriteLongAsync(this Stream stream, long val)
        {
            var buffer = BitConverter.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        public static async Task<long> ReadLongAsync(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            await stream.ReadAsync(buffer, 0, buffer.Length);

            return BitConverter.ToInt64(buffer, 0);
        }

        public static long ReadLong(this Stream stream)
        {
            var buffer = new byte[sizeof(long)];
            stream.Read(buffer, 0, buffer.Length);
            return BitConverter.ToInt64(buffer, 0);
        }

        public static async Task WriteUtf8StringAsync(this Stream stream, string val)
        {
            var buffer = Encoding.UTF8.GetBytes(val);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        public static async Task<string> ReadUtf8StringAsync(this Stream stream, int lengthInBytes)
        {
            var buffer = new byte[lengthInBytes];
            await stream.ReadAsync(buffer, 0, buffer.Length);
            return Encoding.UTF8.GetString(buffer);
        }

        public static string ReadUtf8String(this Stream stream, int lengthInBytes)
        {
            var buffer = new byte[lengthInBytes];
            stream.Read(buffer, 0, buffer.Length);
            return Encoding.UTF8.GetString(buffer);
        }
    }
}
