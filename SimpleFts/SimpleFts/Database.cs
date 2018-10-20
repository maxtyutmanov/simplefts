using System;
using System.Collections.Generic;
using System.Text;
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

        public async Task AddDocument(Document doc)
        {
            long chunkOffset = await _dataFile.AddDocumentAndGetChunkOffset(doc);
            await _index.AddDocument(doc, chunkOffset);
        }

        public void Dispose()
        {
            _dataFile.Dispose();
            _index.Dispose();
        }
    }
}
