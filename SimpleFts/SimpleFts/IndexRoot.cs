using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class IndexRoot
    {
        private readonly ConcurrentDictionary<string, Lazy<FieldIndex>> _fieldIndexes;
        private readonly string _indexDir;

        public IndexRoot(string indexDir)
        {
            _indexDir = indexDir;
            Directory.CreateDirectory(_indexDir);
            _fieldIndexes = InitFieldIndexes();
        }

        public async Task AddDocument(Document d, long dataFileOffset)
        {
            foreach (var field in d.Fields)
            {
                var fieldIndex = GetFieldIndex(field.Key);

                foreach (var term in TokenizeValue(field.Value))
                {
                    await fieldIndex.AddTerm(term, dataFileOffset);
                }
            }
        }

        public IEnumerable<long> Search(SearchQuery query)
        {
            var fix = GetFieldIndex(query.Field);
            return fix.Search(query.Term);
        }

        public async Task Commit()
        {
            var commitTasks = _fieldIndexes.Values.Select(fix => fix.Value.Commit());
            await Task.WhenAll(commitTasks);
        }

        private ConcurrentDictionary<string, Lazy<FieldIndex>> InitFieldIndexes()
        {
            var result = new ConcurrentDictionary<string, Lazy<FieldIndex>>();

            foreach (var dir in Directory.EnumerateDirectories(_indexDir))
            {
                var fieldName = Path.GetFileName(dir);
                var fix = new FieldIndex(_indexDir, fieldName);
                result.TryAdd(fieldName, new Lazy<FieldIndex>(fix));
            }

            return result;
        }

        private IEnumerable<string> TokenizeValue(string value)
        {
            // TODO: implement
            yield return value;
        }

        private FieldIndex GetFieldIndex(string fieldName)
        {
            var lazy = _fieldIndexes.GetOrAdd(
                fieldName, 
                (existingFieldName) => new Lazy<FieldIndex>(() => new FieldIndex(_indexDir, fieldName)));

            return lazy.Value;
        }
    }
}
