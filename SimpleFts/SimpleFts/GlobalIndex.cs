using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class GlobalIndex : IDisposable
    {
        private readonly Dictionary<string, FieldIndex> _fieldIndexes = new Dictionary<string, FieldIndex>();

        public GlobalIndex(string indexDir)
        {

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
            
        }

        private IEnumerable<string> TokenizeValue(string value)
        {
            // TODO: implement
            yield break;
        }

        private FieldIndex GetFieldIndex(string fieldName)
        {
            return _fieldIndexes.GetOrAdd(fieldName, () => new FieldIndex(fieldName));
        }
    }
}
