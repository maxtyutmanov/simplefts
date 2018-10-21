using FluentAssertions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace SimpleFts.Tests
{
    public class FieldIndexTests : IDisposable
    {
        private const string RootIndexDir = @".\index_root";

        private FieldIndex _fix;

        public FieldIndexTests()
        {
            if (Directory.Exists(RootIndexDir))
            {
                Directory.Delete(RootIndexDir, true);
            }
        }

        public void Dispose()
        {
            _fix?.Dispose();
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
    }
}
