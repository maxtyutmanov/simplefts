using SimpleFts.Core.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts.Core
{
    public class DataFileOffsets
    {
        public Dictionary<long, List<Document>> DocsByOffsets { get; set; } = new Dictionary<long, List<Document>>();

        public long LastBatchOffset { get; private set; }

        public void Record(long offset, List<Document> batch)
        {
            DocsByOffsets[offset] = batch;

            if (offset > LastBatchOffset)
            {
                LastBatchOffset = offset;
            }
        }
    }

    public class DataFile : IDisposable
    {
        private const int DefaultBatchSize = 64;

        private readonly ConcurrentQueue<Document> _nonPersistedQ = new ConcurrentQueue<Document>();
        private readonly FileStream _file;
        private readonly string _filePath;
        private readonly SemaphoreSlim _commitLock;

        public DataFile(string filePath, int batchSize = DefaultBatchSize)
        {
            _filePath = filePath;
            _file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        public async Task<DataFileOffsets> Apply(Tran tran)
        {
            foreach (var doc in tran.Documents)
            {
                _nonPersistedQ.Enqueue(doc);
            }

            return await CommitIfRequired().ConfigureAwait(false);
        }

        public async Task<List<Document>> GetChunk(long offset)
        {
            if (offset >= _file.Length)
            {
                // should NEVER happen
                throw new InvalidOperationException("Specified position doesn't exists in data file");
            }

            using (var file = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                file.Position = offset;
                var batch = await DocumentSerializer.DeserializeBatch(file).ConfigureAwait(false);
                return batch;
            }
        }

        public void Dispose()
        {
            _file.Dispose();
        }

        private async Task<DataFileOffsets> CommitIfRequired()
        {
            if (_nonPersistedQ.Count < DefaultBatchSize)
            {
                return null;
            }

            await _commitLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_nonPersistedQ.Count < DefaultBatchSize)
                {
                    return null;
                }

                return await Commit().ConfigureAwait(false);
            }
            finally
            {
                _commitLock.Release();
            }
        }

        private async Task<DataFileOffsets> Commit()
        {
            var offsets = new DataFileOffsets();

            while (_nonPersistedQ.Count >= DefaultBatchSize)
            {
                var batchStartPos = _file.Position;

                var batch = ReadNextBatchFromQueue();
                await DocumentSerializer.SerializeBatch(batch, _file).ConfigureAwait(false);

                offsets.Record(batchStartPos, batch);
            }

            return offsets;
        }

        private List<Document> ReadNextBatchFromQueue()
        {
            var batch = new List<Document>(DefaultBatchSize);

            while (batch.Count < DefaultBatchSize)
            {
                var dequeuedOk = _nonPersistedQ.TryDequeue(out var doc);
                if (!dequeuedOk)
                {
                    // should NEVER happen
                    throw new InvalidOperationException("Could not dequeue item from the nonPersistedQ");
                }
                batch.Add(doc);
            }

            return batch;
        }
    }
}
