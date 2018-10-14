using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class GlobalIndex
    {
        private readonly Dictionary<string, FieldIndex> _fieldIndexes = new Dictionary<string, FieldIndex>();

        public async Task AddDocument(Document d, long dataFileOffset)
        {
            foreach (var field in d.Fields)
            {
                var fieldIndex = GetFieldIndex(field.Key);
                
                foreach (var term in field.Value)
                {
                    await fieldIndex.AddTerm(term, dataFileOffset);
                }
            }
        }

        private FieldIndex GetFieldIndex(string fieldName)
        {
            return _fieldIndexes.GetOrAdd(fieldName, () => new FieldIndex(fieldName));
        }
    }
}
