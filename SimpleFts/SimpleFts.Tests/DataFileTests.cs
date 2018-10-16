using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        public async Task AddSingleDocumentAndGetItBack_ShouldBeOriginalDocument()
        {
            _df = new DataFile(DataDir);
            var doc = new Document();
            doc.SetField("name", "john");

            var offset = await _df.AddDocumentAndGetChunkOffset(doc);

            var chunk = await _df.GetChunk(offset);

            chunk.Should().BeEquivalentTo(new[] { doc });
        }

        [Fact]
        public async Task AddToTwoChunksAndGetFromBoth_ShouldBeOriginalDocuments()
        {
            _df = new DataFile(DataDir, 1);

            var doc1 = new Document();
            doc1.SetField("name", "john");

            var doc2 = new Document();
            doc2.SetField("name", "jack");

            var offsetOfChunk1 = await _df.AddDocumentAndGetChunkOffset(doc1);
            var offsetOfChunk2 = await _df.AddDocumentAndGetChunkOffset(doc2);

            var chunk1 = await _df.GetChunk(offsetOfChunk1);
            var chunk2 = await _df.GetChunk(offsetOfChunk2);

            chunk1.Should().BeEquivalentTo(new[] { doc1 });
            chunk2.Should().BeEquivalentTo(new[] { doc2 });
        }
    }
}
