using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts.Core
{
    public class Database : IDisposable
    {
        private readonly Wal _wal;
        private readonly DataFile _dataFile;
        private readonly IndexRoot _index;

        public Database(string dataDir, string indexDir)
        {
            // TODO: recovery phase

            Directory.CreateDirectory(dataDir);

            var logPath = Path.Combine(dataDir, "log.dat");
            var datafilePath = Path.Combine(dataDir, "data.dat");

            _wal = new Wal(logPath);
            _dataFile = new DataFile(datafilePath);
            _index = new IndexRoot(indexDir);
        }

        public async Task AddDocument(Document doc)
        {
            var tran = Tran.WithSingleDocument(doc);
            await CommitTran(tran).ConfigureAwait(false);
        }

        public async Task AddDocuments(IReadOnlyCollection<Document> docs)
        {
            var tran = Tran.WithDocuments(docs);
            await CommitTran(tran).ConfigureAwait(false);
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

            foreach (var matchingDoc in _dataFile.SearchNonPersistedDocs(doc => grep.IsMatch(query, doc)))
            {
                yield return matchingDoc;
            }
        }

        public void Dispose()
        {
            _wal.Dispose();
            _dataFile.Dispose();
        }

        private async Task CommitTran(Tran tran)
        {
            // persist in write ahead log
            await _wal.Commit(tran).ConfigureAwait(false);

            // add new record to datafile (it may not get commited to disk right away)
            var offsetsMap = await _dataFile.Apply(tran).ConfigureAwait(false);

            // update inverted index
            await UpdateIndex(offsetsMap).ConfigureAwait(false);
        }

        private async Task UpdateIndex(DataFileOffsets offsetsMap)
        {
            if (offsetsMap != null)
            {
                foreach (var entry in offsetsMap.DocsByOffsets)
                {
                    var offset = entry.Key;

                    foreach (var d in entry.Value)
                    {
                        await _index.AddDocument(d, offset).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
