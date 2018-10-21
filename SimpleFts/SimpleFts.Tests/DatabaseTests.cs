using FluentAssertions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SimpleFts.Tests
{
    public class DatabaseTests : IDisposable
    {
        private const string RootIndexDir = @".\DatabaseTests\index_root";
        private const string DataDir = @".\DatabaseTests\datadir";
        private Database _db;

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
    }
}
