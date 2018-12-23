using FluentAssertions;
using SimpleFts.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace SimpleFts.Tests
{
    public class DatabaseTests : IDisposable
    {
        private const string RootIndexDir = @".\DatabaseTests\index_root";
        private const string DataDir = @".\DatabaseTests\datadir";
        private Database _db;
        private int _sequenceId = 1;
        private readonly object _sequenceLock = new object();

        public DatabaseTests()
        {
            if (Directory.Exists(RootIndexDir))
            {
                Directory.Delete(RootIndexDir, true);
            }

            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        public void Dispose()
        {
            _db?.Dispose();
        }

        [Fact]
        public async Task AddToDbAndSearch_ShouldFindDocumentsByDifferentFields()
        {
            _db = new Database(DataDir, RootIndexDir);

            var doc1 = new Document();
            doc1.Fields.Add("id", "idOfDoc1");
            doc1.Fields.Add("name", "john");
            doc1.Fields.Add("favoriteAnimal", "seal");

            var doc2 = new Document();
            doc2.Fields.Add("id", "idOfDoc2");
            doc2.Fields.Add("name", "jack");
            doc2.Fields.Add("favoriteAnimal", "seal");

            await _db.AddDocument(doc1);
            await _db.AddDocument(doc2);

            await _db.Commit();

            _db.Search(new SearchQuery()
            {
                Field = "name",
                Term = "john"
            }).Should().BeEquivalentTo(new[] { doc1 });

            _db.Search(new SearchQuery()
            {
                Field = "name",
                Term = "jack"
            }).Should().BeEquivalentTo(new[] { doc2 });

            _db.Search(new SearchQuery()
            {
                Field = "favoriteAnimal",
                Term = "seal"
            }).Should().BeEquivalentTo(new[] { doc1, doc2 });
        }

        [Fact]
        public async Task AddToDbWithMultipleCommitsAndSearch_ShouldFindDocumentsByDifferentFields()
        {
            _db = new Database(DataDir, RootIndexDir);

            var doc1 = new Document();
            doc1.Fields.Add("id", "idOfDoc1");
            doc1.Fields.Add("name", "john");
            doc1.Fields.Add("favoriteAnimal", "seal");

            var doc2 = new Document();
            doc2.Fields.Add("id", "idOfDoc2");
            doc2.Fields.Add("name", "jack");
            doc2.Fields.Add("favoriteAnimal", "seal");

            await _db.AddDocument(doc1);
            await _db.Commit();

            await _db.AddDocument(doc2);
            await _db.Commit();

            _db.Search(new SearchQuery()
            {
                Field = "name",
                Term = "john"
            }).Should().BeEquivalentTo(new[] { doc1 });

            _db.Search(new SearchQuery()
            {
                Field = "name",
                Term = "jack"
            }).Should().BeEquivalentTo(new[] { doc2 });

            _db.Search(new SearchQuery()
            {
                Field = "favoriteAnimal",
                Term = "seal"
            }).Should().BeEquivalentTo(new[] { doc1, doc2 });
        }

        [Fact]
        public async Task MultithreadedAddAndGet_ShouldGetWhatJustAdded()
        {
            const int numOfThreads = 5;
            const int numOfDocsInTestSet = 50;

            var rand = new Random();
            _db = new Database(DataDir, RootIndexDir);

            var testSets = new Document[numOfThreads][];

            for (int i = 0; i < numOfThreads; i++)
            {
                testSets[i] = GenerateTestSet(rand, numOfDocsInTestSet).ToArray();
            }

            var tasks = testSets.Select(ExecuteTestSetInSeparateThread);

            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task OpenExistingDatabase_ShouldBeTheSameAsInitial()
        {
            _db = new Database(DataDir, RootIndexDir);

            var doc1 = new Document();
            doc1.Fields.Add("id", "idOfDoc1");
            doc1.Fields.Add("name", "john");
            doc1.Fields.Add("favoriteAnimal", "seal");

            var doc2 = new Document();
            doc2.Fields.Add("id", "idOfDoc2");
            doc2.Fields.Add("name", "jack");
            doc2.Fields.Add("favoriteAnimal", "seal");

            await _db.AddDocument(doc1);
            await _db.AddDocument(doc2);

            _db.Dispose();
            _db = new Database(DataDir, RootIndexDir);

            _db.Search(new SearchQuery()
            {
                Field = "favoriteAnimal",
                Term = "seal"
            }).Should().BeEquivalentTo(new[] { doc1, doc2 });
        }

        [Fact]
        public async Task OpenExistingDatabase_AddNewDoc_ShouldFindBothOldAndNewDocs()
        {
            _db = new Database(DataDir, RootIndexDir);

            var doc1 = new Document();
            doc1.Fields.Add("id", "idOfDoc1");
            doc1.Fields.Add("name", "john");
            doc1.Fields.Add("favoriteAnimal", "seal");

            await _db.AddDocument(doc1);

            _db.Dispose();
            _db = new Database(DataDir, RootIndexDir);

            var doc2 = new Document();
            doc2.Fields.Add("id", "idOfDoc2");
            doc2.Fields.Add("name", "jack");
            doc2.Fields.Add("favoriteAnimal", "seal");

            await _db.AddDocument(doc2);

            _db.Search(new SearchQuery()
            {
                Field = "favoriteAnimal",
                Term = "seal"
            }).Should().BeEquivalentTo(new[] { doc1, doc2 });
        }

        private async Task ExecuteTestSetInSeparateThread(Document[] testSet)
        {
            await Task.Run(async () =>
            {
                // write everything to the database
                foreach (var doc in testSet)
                {
                    await _db.AddDocument(doc);
                }

                await _db.Commit();

                // search all documents that were just added in the database
                foreach (var expectedDoc in testSet)
                {
                    var foundDocs = _db.Search(new SearchQuery()
                    {
                        Field = "id",
                        Term = expectedDoc.Fields["id"]
                    });

                    foundDocs.Should().NotBeEmpty();
                    foundDocs.First().Should().BeEquivalentTo(expectedDoc);
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
            doc.Fields.Add("id", GetNextSequenceId().ToString());
            doc.Fields.Add("field1", $"{randNum1} rand value {randNum1}");
            doc.Fields.Add("field2", $"{randNum2} another rand value {randNum2}");

            if (randNum2 > 500)
            {
                doc.Fields.Add("field3", $"{randNum2 / 2} another rand value {randNum2 / 2}");
            }

            return doc;
        }

        private int GetNextSequenceId()
        {
            lock (_sequenceLock)
            {
                return _sequenceId++;
            }
        }
    }
}
