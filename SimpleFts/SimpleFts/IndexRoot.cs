using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class IndexRoot : IDisposable
    {
        private readonly Dictionary<string, FieldIndex> _fieldIndexes;
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

        public void Dispose()
        {
            foreach (var fix in _fieldIndexes.Values)
            {
                fix.Dispose();
            }
        }

        private Dictionary<string, FieldIndex> InitFieldIndexes()
        {
            var result = new Dictionary<string, FieldIndex>();

            try
            {
                foreach (var dir in Directory.EnumerateDirectories(_indexDir))
                {
                    var fieldName = Path.GetFileName(dir);
                    var fix = new FieldIndex(_indexDir, fieldName);
                    result.Add(fieldName, fix);
                }
            }
            catch (Exception)
            {
                foreach (var fix in result.Values)
                {
                    fix.Dispose();
                }
                throw;
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
            return _fieldIndexes.GetOrAdd(fieldName, () => new FieldIndex(_indexDir, fieldName));
        }
    }
}
