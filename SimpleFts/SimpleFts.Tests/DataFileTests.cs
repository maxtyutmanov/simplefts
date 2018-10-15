using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace SimpleFts.Tests
{
    public class DataFileTests : IDisposable
    {
        private const string DataDir = @".\datadir";

        private DataFile _df;

        public DataFileTests()
        {
            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        public void Dispose()
        {
            _df?.Dispose();
            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        [Fact]
        public void AddSingleDocumentAndGetItBack_ShouldBeOriginalDocument()
        {
            _df = new DataFile(DataDir);
            var doc = new Document();
            doc.AddField("name", "john");

            var offset = _df.AddDocumentAndGetChunkOffset(doc);

            var chunk = _df.GetChunk(offset).ToArray();

            chunk.Should().BeEquivalentTo(new[] { doc });
        }

        [Fact]
        public void AddToTwoChunksAndGetFromBoth_ShouldBeOriginalDocuments()
        {
            _df = new DataFile(DataDir, 1);

            var doc1 = new Document();
            doc1.AddField("name", "john");

            var doc2 = new Document();
            doc2.AddField("name", "jack");

            var offsetOfChunk1 = _df.AddDocumentAndGetChunkOffset(doc1);
            var offsetOfChunk2 = _df.AddDocumentAndGetChunkOffset(doc2);

            var chunk1 = _df.GetChunk(offsetOfChunk1);
            var chunk2 = _df.GetChunk(offsetOfChunk2);

            chunk1.Should().BeEquivalentTo(new[] { doc1 });
            chunk2.Should().BeEquivalentTo(new[] { doc2 });
        }
    }
}
