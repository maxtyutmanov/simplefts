using FluentAssertions;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SimpleFts.Tests
{
    public class CompressionTests
    {
        private static UTF8Encoding Encoding = new UTF8Encoding(false);

        [Fact]
        public async Task CompressedAppendToEmptyStreamAndDecompress()
        {
            var sourceStr = "TEST this is TEST";

            var utils = new CompressionUtils();

            using (var source = GetStreamWithContents(sourceStr))
            using (var target = new MemoryStream())
            {
                await utils.CopyWithCompression(source, target);
                var bytes = (await utils.ReadWithDecompression(target, 0)).ToArray();
                var decompressedStr = Encoding.GetString(bytes);

                sourceStr.Should().BeEquivalentTo(decompressedStr);
            }
        }

        [Fact]
        public async Task CompressedAppendToNonEmptyStreamAndDecompress()
        {
            var sourceStr1 = "TEST this is TEST";
            var sourceStr2 = "THIS IS another TEST";

            var utils = new CompressionUtils();

            using (var source1 = GetStreamWithContents(sourceStr1))
            using (var source2 = GetStreamWithContents(sourceStr2))
            using (var target = new MemoryStream())
            {
                var chunk1Position = 0;
                await utils.CopyWithCompression(source1, target);
                var chunk2Position = target.Position;
                await utils.CopyWithCompression(source2, target);

                var decompressedBytes1 = (await utils.ReadWithDecompression(target, chunk1Position)).ToArray();
                var decompressedStr1 = Encoding.GetString(decompressedBytes1);
                var decompressedBytes2 = (await utils.ReadWithDecompression(target, chunk2Position)).ToArray();
                var decompressedStr2 = Encoding.GetString(decompressedBytes2);

                sourceStr1.Should().Be(decompressedStr1);
                sourceStr2.Should().Be(decompressedStr2);
            }
        }

        private static Stream GetStreamWithContents(string contents)
        {
            var ms = new MemoryStream();

            using (var writer = new StreamWriter(ms, Encoding, 1024, true))
            {
                writer.Write(contents);
            }

            return ms;
        }
    }
}
