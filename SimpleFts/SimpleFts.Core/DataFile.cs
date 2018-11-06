using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimpleFts.Core.Serialization;
using SimpleFts.Core.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class DataFile : IDisposable
    {
        private const int DefaultChunkSize = 64;

        private readonly string _bufferPath;
        private readonly string _mainPath;
        private readonly FileStream _diskBuffer;
        private readonly FileStream _main;
        private readonly int _chunkSize;
        private readonly ConcurrentQueue<Document> _inMemoryBuffer;
        
        // TODO: replace with async rwlock
        private readonly SemaphoreSlim _bufferLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _addLock = new SemaphoreSlim(1, 1);

        private long _currentLengthOfMain;

        public DataFile(string dataDir, int chunkSize = DefaultChunkSize)
        {
            Directory.CreateDirectory(dataDir);

            _bufferPath = Path.Combine(dataDir, "buffer.dat");
            _mainPath = Path.Combine(dataDir, "main.dat");

            _diskBuffer = new FileStream(_bufferPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            _diskBuffer.Position = _diskBuffer.Length;
            _main = new FileStream(_mainPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _main.Position = _main.Length;

            _chunkSize = chunkSize;

            _inMemoryBuffer = new ConcurrentQueue<Document>(ReadDocumentsFromDiskBuffer().Result);
            _currentLengthOfMain = _main.Length;
        }

        public async Task<long> AddDocumentAndGetChunkOffset(Document d)
        {
            return await AddDocumentsAndGetChunkOffset(new List<Document>() { d }, CancellationToken.None);
        }

        public async Task<long> AddDocumentsAndGetChunkOffset(IReadOnlyCollection<Document> documents, CancellationToken ct)
        {
            // we don't allow adding documents in parallel (it doesn't make sense anyway because of IO involved)

            await _addLock.WaitAsync().ConfigureAwait(false);

            try
            {
                await FlushBufferIfOverflown().ConfigureAwait(false);
                await AppendDocumentsToBuffer(documents, ct).ConfigureAwait(false);

                return _currentLengthOfMain;
            }
            finally
            {
                _addLock.Release();
            }
        }

        public void Dispose()
        {
            _addLock.Dispose();
            _bufferLock.Dispose();
            _diskBuffer.Dispose();
            _main.Dispose();
        }

        public async Task<List<Document>> GetChunk(long chunkOffset)
        {
            if (IsInMainFile(chunkOffset))
            {
                return await ReadDocumentsFromMain(chunkOffset).ConfigureAwait(false);
            }

            await _bufferLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (IsInBuffer(chunkOffset))
                {
                    return ReadDocumentsFromMemoryBuffer();
                }
                else
                {
                    return await ReadDocumentsFromMain(chunkOffset).ConfigureAwait(false);
                }
            }
            finally
            {
                _bufferLock.Release();
            }
        }

        private async Task AppendDocumentsToBuffer(IReadOnlyCollection<Document> docs, CancellationToken ct)
        {
            using (Measured.Operation("append_document_to_buffer"))
            {
                foreach (var doc in docs)
                {
                    _inMemoryBuffer.Enqueue(doc);
                }

                await DocumentSerializer.SerializeBatch(docs, _diskBuffer).ConfigureAwait(false);
                await _diskBuffer.FlushAsync().ConfigureAwait(false);
            }
        }

        private List<Document> ReadDocumentsFromMemoryBuffer()
        {
            return _inMemoryBuffer.ToList();
        }

        private async Task<List<Document>> ReadDocumentsFromDiskBuffer()
        {
            var serializer = new JsonSerializer();

            using (Measured.Operation("read_documents_from_buffer"))
            using (var buffer = new FileStream(_bufferPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                return await ReadDocumentsFromDecompressedStream(buffer);
            }
        }

        private async Task<List<Document>> ReadDocumentsFromMain(long chunkOffset)
        {
            var compUtils = new CompressionUtils();

            using (Measured.Operation("read_documents_from_buffer"))
            using (var main = new FileStream(_mainPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
            {
                var chunk = await compUtils.ReadWithDecompression(main, chunkOffset).ConfigureAwait(false);
                using (var ms = new MemoryStream(chunk.Array, chunk.Offset, chunk.Count))
                {
                    return await ReadDocumentsFromDecompressedStream(ms);
                }
            }
        }

        private async Task<List<Document>> ReadDocumentsFromDecompressedStream(Stream stream)
        {
            var result = new List<Document>(_chunkSize);

            while (stream.NotEof())
            {
                var batch = await DocumentSerializer.DeserializeBatch(stream).ConfigureAwait(false);
                result.AddRange(batch);
            }

            return result;
        }

        private async Task FlushBufferIfOverflown()
        {
            if (_inMemoryBuffer.Count >= _chunkSize)
            {
                await _bufferLock.WaitAsync().ConfigureAwait(false);

                try
                {
                    if (_inMemoryBuffer.Count >= _chunkSize)
                    {
                        await FlushBuffer().ConfigureAwait(false);
                    }
                }
                finally
                {
                    _bufferLock.Release();
                }
            }
        }

        private async Task FlushBuffer()
        {
            if (_inMemoryBuffer.Count == 0)
                return;

            using (Measured.Operation("flush_datafile_buffer"))
            using (var ms = new MemoryStream())
            {
                var tmpBuffer = ReadDocumentsFromMemoryBuffer();
                var compUtils = new CompressionUtils();

                await DocumentSerializer.SerializeBatch(tmpBuffer, ms).ConfigureAwait(false);
                await compUtils.CopyWithCompression(ms, _main).ConfigureAwait(false);
                await _main.FlushAsync().ConfigureAwait(false);
                _currentLengthOfMain = _main.Length;
                // truncate disk buffer
                _diskBuffer.SetLength(0);

                for (int i = 0; i < tmpBuffer.Count; i++)
                {
                    _inMemoryBuffer.TryDequeue(out _);
                }
            }
        }

        private bool IsInMainFile(long offset) => offset < _currentLengthOfMain;

        private bool IsInBuffer(long offset) => !IsInMainFile(offset);
    }
}
