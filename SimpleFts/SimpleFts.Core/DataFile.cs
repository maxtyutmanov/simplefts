using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimpleFts.Core.Serialization;
using SimpleFts.Core.Utils;
using System;
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
        private readonly FileStream _buffer;
        private readonly FileStream _main;
        private readonly int _chunkSize;
        
        // TODO: replace with async rwlock
        private readonly SemaphoreSlim _bufferLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _addLock = new SemaphoreSlim(1, 1);

        private int _docsInCurrentChunk = 0;
        private long _currentLengthOfMain;

        public DataFile(string dataDir, int chunkSize = DefaultChunkSize)
        {
            Directory.CreateDirectory(dataDir);

            _bufferPath = Path.Combine(dataDir, "buffer.dat");
            _mainPath = Path.Combine(dataDir, "main.dat");

            _buffer = new FileStream(_bufferPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            _buffer.Position = _buffer.Length;
            _main = new FileStream(_mainPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _main.Position = _main.Length;

            _chunkSize = chunkSize;

            _docsInCurrentChunk = ReadDocumentsFromBuffer().Result.Count;
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
            _buffer.Dispose();
            _main.Dispose();
        }

        public async Task<List<Document>> GetChunk(long chunkOffset)
        {
            // check whether the chunk by the requested offset is in main file or in buffer
            if (chunkOffset >= _currentLengthOfMain)
            {
                await _bufferLock.WaitAsync().ConfigureAwait(false);

                try
                {
                    // we have to double-check this: what if it WAS in buffer, but the buffer was flushed immediately after our check
                    if (chunkOffset >= _currentLengthOfMain)
                    {
                        // this chunk is not yet flushed from the buffer into the main data file
                        return await ReadDocumentsFromBuffer().ConfigureAwait(false);
                    }
                }
                finally
                {
                    _bufferLock.Release();
                }
            }

            return await ReadDocumentsFromMain(chunkOffset).ConfigureAwait(false);
        }

        private async Task AppendDocumentsToBuffer(IReadOnlyCollection<Document> docs, CancellationToken ct)
        {
            using (Measured.Operation("append_document_to_buffer"))
            {
                await DocumentSerializer.SerializeBatch(docs, _buffer).ConfigureAwait(false);
                await _buffer.FlushAsync().ConfigureAwait(false);
                _docsInCurrentChunk += docs.Count;
            }
        }

        private async Task<List<Document>> ReadDocumentsFromBuffer()
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
            if (_docsInCurrentChunk >= _chunkSize)
            {
                await _bufferLock.WaitAsync().ConfigureAwait(false);

                try
                {
                    if (_docsInCurrentChunk >= _chunkSize)
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
            using (Measured.Operation("flush_datafile_buffer"))
            {
                var compUtils = new CompressionUtils();

                if (_buffer.Length == 0)
                {
                    return;
                }

                await compUtils.CopyWithCompression(_buffer, _main).ConfigureAwait(false);
                await _main.FlushAsync().ConfigureAwait(false);
                _currentLengthOfMain = _main.Length;
                _docsInCurrentChunk = 0;
                // truncate buffer
                _buffer.SetLength(0);
            }
        }
    }
}
