using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class Database : IDisposable
    {
        private readonly DataFile _dataFile;
        private readonly IndexRoot _index;

        public Database(string dataDir, string indexDir)
        {
            _dataFile = new DataFile(dataDir);
            _index = new IndexRoot(indexDir);
        }

        public async Task AddDocumentsBatch(IReadOnlyCollection<Document> batch, CancellationToken ct)
        {
            long chunkOffset = await _dataFile.AddDocumentsAndGetChunkOffset(batch, ct);

            foreach (var doc in batch)
            {
                await _index.AddDocument(doc, chunkOffset);
            }
        }

        public async Task AddDocument(Document doc)
        {
            long chunkOffset = await _dataFile.AddDocumentAndGetChunkOffset(doc);
            await _index.AddDocument(doc, chunkOffset);
        }

        public async Task Commit()
        {
            await _index.Commit();
        }

        public IEnumerable<Document> Search(SearchQuery query)
        {
            var chunkOffsets = _index.Search(query);
            var grep = new Grep();

            foreach (var chunkOffset in chunkOffsets)
            {
                var chunk = _dataFile.GetChunk(chunkOffset).GetAwaiter().GetResult();
                
                foreach (var matchingDoc in grep.Filter(query, chunk))
                {
                    yield return matchingDoc;
                }
            }
        }

        public void Dispose()
        {
            _dataFile.Dispose();
        }
    }
}
