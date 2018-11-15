using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Core
{
    public class Tran
    {
        private readonly Queue<Document> _docs = new Queue<Document>();

        public void AddDocument(Document doc)
        {
            _docs.Enqueue(doc);
        }

        public IReadOnlyCollection<Document> Documents => _docs;
    }
}
