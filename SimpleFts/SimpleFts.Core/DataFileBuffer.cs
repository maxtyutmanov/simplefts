using SimpleFts.Core.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts.Core
{
    public class DataFileBuffer : IDisposable
    {
        private const int DefaultChunkSize = 64;

        private readonly FileStream _diskBuffer;
        private readonly ConcurrentQueue<Document> _inMemoryBuffer;
        private readonly Stream _main;
        private readonly int _chunkSize;
        private readonly SemaphoreSlim _addLock = new SemaphoreSlim(1, 1);

        public DataFileBuffer(string bufferFilePath, Stream main, int chunkSize = DefaultChunkSize)
        {
            _diskBuffer = new FileStream(bufferFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

            var docsInDiskBuffer = ReadDocumentsFromDiskBuffer();
            _inMemoryBuffer = new ConcurrentQueue<Document>(docsInDiskBuffer);
            _main = main ?? throw new ArgumentNullException(nameof(main));
            _chunkSize = chunkSize;
        }

        public async Task<bool> AddDocuments(IReadOnlyCollection<Document> docs)
        {
            await _addLock.WaitAsync().ConfigureAwait(false);

            try
            {
                foreach (var doc in docs)
                {
                    _inMemoryBuffer.Enqueue(doc);
                }

                await DocumentSerializer.SerializeBatch(docs, _diskBuffer).ConfigureAwait(false);
                await _diskBuffer.FlushAsync().ConfigureAwait(false);

                return await FlushToMainIfOverflown().ConfigureAwait(false);
            }
            finally
            {
                _addLock.Release();
            }
        }

        public List<Document> ReadAll()
        {
            return _inMemoryBuffer.ToList();
        }

        public void Dispose()
        {
            _diskBuffer.Dispose();
        }

        private IEnumerable<Document> ReadDocumentsFromDiskBuffer()
        {
            return DocumentSerializer.DeserializeFromStream(_diskBuffer).Result.ToList();
        }

        private async Task<bool> FlushToMainIfOverflown()
        {
            if (_inMemoryBuffer.Count >= _chunkSize)
            {
                await FlushToMain().ConfigureAwait(false);
                return true;
            }

            return false;
        }

        private async Task FlushToMain()
        {
            // take snapshot of in-memory buffer
            var bufferSnapshot = new List<Document>(_inMemoryBuffer);

            await DocumentSerializer.SerializeBatch(_inMemoryBuffer, _main).ConfigureAwait(false);
            await _main.FlushAsync().ConfigureAwait(false);
            _diskBuffer.SetLength(0);

            // if successful, dequeue just-serialized items from the in-memory buffer
            for (int i = 0; i < bufferSnapshot.Count; i++)
            {
                _inMemoryBuffer.TryDequeue(out _);
            }
        }
    }
}
