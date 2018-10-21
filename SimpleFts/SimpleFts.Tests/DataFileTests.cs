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
        private const string DataDir = @".\DataFileTests\datadir";

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

        [Fact]
        public async Task MultithreadedAddAndGet_ShouldGetWhatJustAdded()
        {
            const int numOfThreads = 10;
            const int numOfDocsInTestSet = 100;

            var rand = new Random();
            _df = new DataFile(DataDir, 16);

            var testSets = new Document[numOfThreads][];

            for (int i = 0; i < numOfThreads; i++)
            {
                testSets[i] = GenerateTestSet(rand, numOfDocsInTestSet).ToArray();
            }

            var tasks = testSets.Select(ExecuteTestSetInSeparateThread);

            await Task.WhenAll(tasks);
        }

        private async Task ExecuteTestSetInSeparateThread(Document[] testSet)
        {
            await Task.Run(async () =>
            {
                var docToOffsetMap = new Dictionary<Document, long>();

                // write everything to the datafile
                foreach (var doc in testSet)
                {
                    var offset = await _df.AddDocumentAndGetChunkOffset(doc);
                    docToOffsetMap[doc] = offset;
                }

                // read by all remembered offsets and make sure documents are really there
                foreach (var expectedDoc in testSet)
                {
                    var offset = docToOffsetMap[expectedDoc];
                    var chunk = await _df.GetChunk(offset);

                    chunk.Should().Contain(doc => AreSameDocuments(expectedDoc, doc), expectedDoc.ToJson());
                }
            });
        }

        private bool AreSameDocuments(Document expectedDoc, Document doc)
        {
            if (doc.Fields.Count != expectedDoc.Fields.Count)
                return false;

            foreach (var expectedPair in expectedDoc.Fields)
            {
                if (!doc.Fields.TryGetValue(expectedPair.Key, out var fieldVal) || fieldVal != expectedPair.Value)
                {
                    return false;
                }
            }

            return true;
        }

        private IEnumerable<Document> GenerateTestSet(Random rand, int size)
        {
            for (int i = 0; i < size; i++)
            {
                yield return GenerateRandomDocument(rand);
            }
        }

        private Document GenerateRandomDocument(Random rand)
        {
            var randNum1 = rand.Next(1000);
            var randNum2 = rand.Next(1000);

            var doc = new Document();
            doc.Fields.Add("field1", $"{randNum1} rand value {randNum1}");
            doc.Fields.Add("field2", $"{randNum2} another rand value {randNum2}");
            
            if (randNum2 > 500)
            {
                doc.Fields.Add("field3", $"{randNum2 / 2} another rand value {randNum2 / 2}");
            }

            return doc;
        }
    }
}
