using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SimpleFts.Tests
{
    [TestClass]
    public class DataFileTests
    {
        private const string DataDir = @".\datadir";

        private DataFile _df;

        [TestInitialize]
        public void Init()
        {
            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            _df?.Dispose();
            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        [TestMethod]
        public void AddSingleDocumentAndGetItBack()
        {
            _df = new DataFile(DataDir);
            var doc = new Document();
            doc.AddField("name", "john");

            var offset = _df.AddDocumentAndGetChunkOffset(doc);

            var chunk = _df.EnumerateChunk(offset).ToArray();

            Assert.AreEqual(1, chunk.Length);

            var readDoc = chunk[0];

            Assert.AreEqual(doc.Fields.Count, readDoc.Fields.Count);
            Assert.AreEqual(doc.Fields["name"].Count, readDoc.Fields["name"].Count);
            Assert.AreEqual(doc.Fields["name"].First(), readDoc.Fields["name"].First());
        }
    }
}
