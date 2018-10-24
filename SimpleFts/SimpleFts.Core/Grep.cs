using SimpleFts.Core.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleFts
{
    public class Grep
    {
        private readonly StringComparer _comparer = StringComparer.OrdinalIgnoreCase;

        public IEnumerable<Document> Filter(SearchQuery query, IEnumerable<Document> docs)
        {
            using (Measured.Operation("grep_filter"))
            {
                foreach (var matchingDoc in docs.Where(doc => IsMatch(query, doc)))
                {
                    yield return matchingDoc;
                }
            }
        }

        private bool IsMatch(SearchQuery query, Document doc)
        {
            if (doc.Fields.TryGetValue(query.Field, out var docFieldValue))
            {
                return _comparer.Equals(query.Term, docFieldValue);
            }
            else
            {
                return false;
            }
        }
    }
}
