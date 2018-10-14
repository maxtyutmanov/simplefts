using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;

namespace SimpleFts.Tests
{
    [TestClass]
    public class CompressionTests
    {
        [TestMethod]
        public void CompressedAppendToEmptyStreamAndDecompress()
        {
            var sourceStr = "TEST this is TEST";

            using (var source = GetStreamWithContents(sourceStr))
            using (var target = new MemoryStream())
            {
                source.CompressAndAppendTo(target);
                var bytes = target.DecompressChunk(0);
                var decompressedStr = Encoding.UTF8.GetString(bytes);

                Assert.AreEqual(sourceStr, decompressedStr);
            }
        }

        private static Stream GetStreamWithContents(string contents)
        {
            var ms = new MemoryStream();

            using (var writer = new StreamWriter(ms, Encoding.UTF8, 1024, true))
            {
                writer.Write(contents);
            }

            return ms;
        }
    }
}
