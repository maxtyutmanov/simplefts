using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Core
{
    public class Tran
    {
        private readonly List<Document> _docs = new List<Document>();

        public static Tran WithDocuments(IEnumerable<Document> docs)
        {
            var tran = new Tran();
            tran._docs.AddRange(docs);
            return tran;
        }

        public static Tran WithSingleDocument(Document doc)
        {
            var tran = new Tran();
            tran.AddDocument(doc);
            return tran;
        }

        public void AddDocument(Document doc)
        {
            _docs.Add(doc);
        }

        public IReadOnlyList<Document> Documents => _docs;
    }
}
