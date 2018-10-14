using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;

namespace SimpleFts.Tests
{
    [TestClass]
    public class CompressionTests
    {
        private static UTF8Encoding Encoding = new UTF8Encoding(false);

        [TestMethod]
        public void CompressedAppendToEmptyStreamAndDecompress()
        {
            var sourceStr = "TEST this is TEST";

            using (var source = GetStreamWithContents(sourceStr))
            using (var target = new MemoryStream())
            {
                source.CompressChunkAndAppendTo(target);
                var bytes = target.DecompressChunk(0);
                var decompressedStr = Encoding.GetString(bytes);

                Assert.AreEqual(sourceStr, decompressedStr);
            }
        }

        [TestMethod]
        public void CompressedAppendToNonEmptyStreamAndDecompress()
        {
            var sourceStr1 = "TEST this is TEST";
            var sourceStr2 = "THIS IS another TEST";

            using (var source1 = GetStreamWithContents(sourceStr1))
            using (var source2 = GetStreamWithContents(sourceStr2))
            using (var target = new MemoryStream())
            {
                var chunk1Position = 0;
                source1.CompressChunkAndAppendTo(target);
                var chunk2Position = target.Position;
                source2.CompressChunkAndAppendTo(target);

                var decompressedBytes1 = target.DecompressChunk(chunk1Position);
                var decompressedStr1 = Encoding.GetString(decompressedBytes1);
                var decompressedBytes2 = target.DecompressChunk(chunk2Position);
                var decompressedStr2 = Encoding.GetString(decompressedBytes2);

                Assert.AreEqual(sourceStr1, decompressedStr1);
                Assert.AreEqual(sourceStr2, decompressedStr2);
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
