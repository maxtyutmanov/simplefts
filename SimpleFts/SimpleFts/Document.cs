using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts
{
    public class Document
    {
        public Dictionary<string, HashSet<string>> Fields { get; } = new Dictionary<string, HashSet<string>>();

        public void AddField(string fieldName, string fieldValue)
        {
            if (Fields.TryGetValue(fieldName, out var existingValues))
            {
                existingValues.Add(fieldValue);
            }
            else
            {
                var values = new HashSet<string>();
                values.Add(fieldValue);
                Fields[fieldName] = values;
            }
        }
    }
}
