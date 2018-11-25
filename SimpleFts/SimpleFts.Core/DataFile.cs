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

namespace SimpleFts.Core
{
    public class DataFileCommitInfo
    {
        public Dictionary<long, List<Document>> DocsByOffsets { get; } = new Dictionary<long, List<Document>>();

        public long LastCommittedDocId
        {
            get
            {
                var maxOffset = DocsByOffsets.Keys.Max();
                var maxDocId = DocsByOffsets[maxOffset].Select(d => d.Id).Max();
                return maxDocId;
            }
        }

        public void RecordCommittedBatch(long offset, List<Document> batch)
        {
            DocsByOffsets[offset] = batch;
        }
    }

    public class DataFile : IDisposable
    {
        private const int DefaultBatchSize = 128;

        private readonly ConcurrentQueue<Document> _nonPersistedQ = new ConcurrentQueue<Document>();
        private readonly FileStream _file;
        private readonly string _filePath;
        private readonly int _batchSize;
        private readonly SemaphoreSlim _commitLock = new SemaphoreSlim(1, 1);

        public DataFile(string filePath, int batchSize = DefaultBatchSize)
        {
            _filePath = filePath;
            _batchSize = batchSize;
            _file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        public async Task<DataFileCommitInfo> Apply(Tran tran)
        {
            foreach (var doc in tran.Documents)
            {
                _nonPersistedQ.Enqueue(doc);
            }

            return await CommitIfRequired().ConfigureAwait(false);
        }

        public async Task<List<Document>> GetChunk(long offset)
        {
            var compression = new CompressionUtils();

            if (offset >= _file.Length)
            {
                // should NEVER happen
                throw new InvalidOperationException("Specified position doesn't exists in data file");
            }

            using (var file = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var segment = await compression.ReadWithDecompression(file, offset);

                using (var ms = new MemoryStream(segment.Array, segment.Offset, segment.Count))
                {
                    var batch = await DocumentSerializer.DeserializeBatch(ms).ConfigureAwait(false);
                    return batch;
                }
            }
        }

        public IEnumerable<Document> SearchNonPersistedDocs(Func<Document, bool> predicate)
        {
            _commitLock.Wait();

            try
            {
                return _nonPersistedQ.Where(predicate).ToList();
            }
            finally
            {
                _commitLock.Release();
            }
        }

        public void Dispose()
        {
            _file.Dispose();
            _commitLock.Dispose();
        }

        private async Task<DataFileCommitInfo> CommitIfRequired()
        {
            if (_nonPersistedQ.Count < _batchSize)
            {
                return null;
            }

            await _commitLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_nonPersistedQ.Count < _batchSize)
                {
                    return null;
                }

                using (Measured.Operation("datafile_commit"))
                {
                    return await Commit().ConfigureAwait(false);
                }
            }
            finally
            {
                _commitLock.Release();
            }
        }

        private async Task<DataFileCommitInfo> Commit()
        {
            var compression = new CompressionUtils();
            var commitInfo = new DataFileCommitInfo();

            while (_nonPersistedQ.Count >= _batchSize)
            {
                var batchStartPos = _file.Position;
                var nextBatch = ReadNextBatchFromQueue();

                using (var ms = new MemoryStream())
                {
                    await DocumentSerializer.SerializeBatch(nextBatch, ms).ConfigureAwait(false);
                    await compression.CopyWithCompression(ms, _file).ConfigureAwait(false);
                    await _file.FlushAsync().ConfigureAwait(false);
                }

                commitInfo.RecordCommittedBatch(batchStartPos, nextBatch);
            }

            return commitInfo;
        }

        private List<Document> ReadNextBatchFromQueue()
        {
            var batch = new List<Document>(_batchSize);

            while (batch.Count < _batchSize)
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
