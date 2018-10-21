using FluentAssertions;
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
    public class FieldIndexTests
    {
        private const string RootIndexDir = @".\FieldIndexTests\index_root";

        private FieldIndex _fix;

        public FieldIndexTests()
        {
            if (Directory.Exists(RootIndexDir))
            {
                Directory.Delete(RootIndexDir, true);
            }
        }

        [Fact]
        public async Task AddedTermCanBeFoundByItsValue()
        {
            _fix = new FieldIndex(RootIndexDir, "text");

            await _fix.AddTerm("term of document 1", 1);
            await _fix.AddTerm("another term of document 1", 1);
            await _fix.AddTerm("term of document 2", 2);
            await _fix.AddTerm("another term of document 2", 2);

            await _fix.Commit();

            _fix.Search("term of document 1")
                .Should().BeEquivalentTo(new[] { 1 });
            _fix.Search("another term of document 1")
                .Should().BeEquivalentTo(new[] { 1 });
            _fix.Search("term of document 2")
                .Should().BeEquivalentTo(new[] { 2 });
            _fix.Search("another term of document 2")
                .Should().BeEquivalentTo(new[] { 2 });
        }

        [Fact]
        public async Task QueryNonExistingTerm_ShouldGetEmptyResultSet()
        {
            _fix = new FieldIndex(RootIndexDir, "text");

            await _fix.AddTerm("term of document 1", 1);

            await _fix.Commit();

            _fix.Search("unknown term").Should().BeEmpty();
        }

        [Fact]
        public async Task CommitEmptyIndex_ShouldNotThrowError()
        {
            _fix = new FieldIndex(RootIndexDir, "text");

            await _fix.Commit();

            _fix.Search("any text").Should().BeEmpty();
        }

        [Fact]
        public async Task AddTermsWithMultipleCommits_AllShouldBeFound()
        {
            _fix = new FieldIndex(RootIndexDir, "text");

            await _fix.AddTerm("term of document 1", 1);
            await _fix.AddTerm("another term of document 1", 1);

            await _fix.Commit();

            await _fix.AddTerm("term of document 2", 2);
            await _fix.AddTerm("another term of document 2", 2);

            await _fix.Commit();

            _fix.Search("term of document 1")
                .Should().BeEquivalentTo(new[] { 1 });
            _fix.Search("another term of document 1")
                .Should().BeEquivalentTo(new[] { 1 });
            _fix.Search("term of document 2")
                .Should().BeEquivalentTo(new[] { 2 });
            _fix.Search("another term of document 2")
                .Should().BeEquivalentTo(new[] { 2 });
        }

        [Fact]
        public async Task AddTermsFromMultipleThreads_AllShouldBeFound()
        {
            const int numOfThreads = 10;
            const int numOfTermsInTestSet = 100;

            _fix = new FieldIndex(RootIndexDir, "text");

            var rand = new Random();
            (string, long)[][] testSets = new (string, long)[numOfThreads][];

            for (int i = 0; i < numOfThreads; i++)
            {
                testSets[i] = GenerateTestSet(rand, numOfTermsInTestSet).ToArray();
            }

            var tasks = testSets.Select(ExecuteTestSetInSeparateThread);

            await Task.WhenAll(tasks);
        }

        private async Task ExecuteTestSetInSeparateThread((string, long)[] testSet)
        {
            await Task.Run(async () =>
            {
                // write everything to the index
                foreach (var entry in testSet)
                {
                    await _fix.AddTerm(entry.Item1, entry.Item2);
                }

                await _fix.Commit();

                // read everything back from the index
                foreach (var entry in testSet)
                {
                    _fix.Search(entry.Item1).Should().Contain(entry.Item2);
                }
            });
        }

        private IEnumerable<(string, long)> GenerateTestSet(Random rand, int size)
        {
            for (int i = 0; i < size; i++)
            {
                yield return GenerateRandomTerm(rand);
            }
        }

        private (string, long) GenerateRandomTerm(Random rand)
        {
            var randomVal1 = rand.Next(100);
            var randomVal2 = rand.Next(1000);

            return ($"#{randomVal1} random term for offset {randomVal2}", randomVal2);
        }
    }
}
