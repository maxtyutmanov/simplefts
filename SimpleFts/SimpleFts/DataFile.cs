using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class DataFile : IDisposable
    {
        private const int DefaultChunkSize = 1024;

        private readonly string _bufferPath;
        private readonly string _mainPath;
        private readonly FileStream _buffer;
        private readonly FileStream _main;
        private readonly int _chunkSize;
        private readonly ReaderWriterLockSlim _bufferLock = new ReaderWriterLockSlim();

        private int _docsInCurrentChunk = 0;

        public DataFile(string dataDir, int chunkSize = DefaultChunkSize)
        {
            Directory.CreateDirectory(dataDir);

            _bufferPath = Path.Combine(dataDir, "buffer.dat");
            _mainPath = Path.Combine(dataDir, "main.dat");

            _buffer = new FileStream(_bufferPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _main = new FileStream(_mainPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);

            _chunkSize = chunkSize;
        }

        public long AddDocumentAndGetChunkOffset(Document d)
        {
            if (_docsInCurrentChunk == DefaultChunkSize)
            {
                FlushBuffer();
            }

            var docStr = JsonConvert.SerializeObject(d);
            var docBytes = Encoding.UTF8.GetBytes(docStr);

            _buffer.Write(docBytes, 0, docBytes.Length);
            _buffer.Flush();

            ++_docsInCurrentChunk;

            return _main.Length;
        }

        public void Dispose()
        {
            _bufferLock.Dispose();
            _buffer.Dispose();
            _main.Dispose();
        }

        public IEnumerable<Document> EnumerateChunk(long chunkOffset)
        {
            if (chunkOffset >= _main.Length)
            {
                return EnumerateBuffer();
            }
            else
            {
                return EnumerateChunkFromMain(chunkOffset);
            }
        }

        private IEnumerable<Document> EnumerateBuffer()
        {
            _bufferLock.EnterReadLock();

            try
            {
                var serializer = new JsonSerializer();

                using (var buffer = new FileStream(_bufferPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var sr = new StreamReader(buffer, Encoding.UTF8, false, 1024, true))
                using (var jr = new JsonTextReader(sr))
                {
                    while (!sr.EndOfStream)
                    {
                        jr.Read();
                        yield return serializer.Deserialize<Document>(jr);
                    }
                }
            }
            finally
            {
                _bufferLock.ExitReadLock();
            }
        }

        private IEnumerable<Document> EnumerateChunkFromMain(long chunkOffset)
        {
            var serializer = new JsonSerializer();

            using (var main = new FileStream(_mainPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
            {
                var chunkBytes = main.DecompressChunk(chunkOffset);

                using (var ms = new MemoryStream(chunkBytes))
                using (var sr = new StreamReader(ms, Encoding.UTF8, false, 1024, true))
                using (var jr = new JsonTextReader(sr))
                {
                    while (!sr.EndOfStream)
                    {
                        jr.Read();
                        yield return serializer.Deserialize<Document>(jr);
                    }
                }
            }
        }

        private void FlushBuffer()
        {
            _bufferLock.EnterWriteLock();

            try
            {
                if (_buffer.Length == 0)
                {
                    return;
                }

                _buffer.CompressChunkAndAppendTo(_main);
                _main.Flush();
                // truncate buffer
                _buffer.SetLength(0);
            }
            finally
            {
                _bufferLock.ExitWriteLock();
            }
        }
    }
}
