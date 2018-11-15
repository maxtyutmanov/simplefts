using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Core
{
    public class Tran
    {
        private readonly Queue<Document> _docs = new Queue<Document>();

        public static Tran WithDocuments(IEnumerable<Document> docs)
        {
            var tran = new Tran();
            foreach (var doc in docs)
            {
                tran.AddDocument(doc);
            }
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
            _docs.Enqueue(doc);
        }

        public IReadOnlyCollection<Document> Documents => _docs;
    }
}
