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
    public class Wal : IDisposable
    {
        private const long DefaultMaxSize = 10 * 1024 * 1024;   // 10 MB
        
        private FileStream _file;
        private readonly SemaphoreSlim _commitLock = new SemaphoreSlim(1, 1);
        private readonly string _walPath;
        private readonly long _maxFileSize;
        private readonly DocIdGenerator _docIdGenerator;


        public Wal(string walPath, long maxFileSize = DefaultMaxSize)
        {
            _file = new FileStream(walPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            _file.Position = _file.Length;
            _walPath = walPath;
            _maxFileSize = maxFileSize;
            _docIdGenerator = new DocIdGenerator(0);
        }

        public async Task CommitTran(Tran tran)
        {
            await _commitLock.WaitAsync().ConfigureAwait(false);

            try
            {
                using (Measured.Operation("wal_commit"))
                {
                    foreach (var doc in tran.Documents)
                    {
                        doc.Id = _docIdGenerator.GetNextId();
                    }

                    await DocumentSerializer.SerializeBatch(tran.Documents, _file).ConfigureAwait(false);
                    await _file.FlushAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                _commitLock.Release();
            }
        }

        public async Task Checkpoint(long docId)
        {
            if (_file.Length < _maxFileSize)
            {
                // log file is not large enough to truncate it
                return;
            }

            await _commitLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_file.Length < _maxFileSize)
                {
                    // log file is not large enough to truncate it
                    return;
                }

                await TruncateByDocId(docId).ConfigureAwait(false);
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

        private async Task TruncateByDocId(long docId)
        {
            using (Measured.Operation("wal_truncate"))
            {
                try
                {
                    _file.Position = _file.Length;

                    while (_file.Position != 0)
                    {
                        var batch = await DocumentSerializer.DeserializeBatchFromRightToLeft(_file);
                        var firstDocIdInBatch = batch[0].Id;

                        if (docId >= firstDocIdInBatch)
                        {
                            await TruncateByFileOffset(_file.Position);
                        }
                    }
                }
                finally
                {
                    _file.Position = _file.Length;
                }
            }
        }

        private async Task TruncateByFileOffset(long offset)
        {
            _file.Position = offset;

            var tmpWalPath = $"{_walPath}.tmp";

            using (var tmpFile = new FileStream(tmpWalPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await _file.CopyToAsync(tmpFile);
            }

            // perform quick switch
            _file.Dispose();
            try
            {
                File.Move(_walPath, $"{_walPath}.old");
                File.Move(tmpWalPath, _walPath);
            }
            finally
            {
                _file = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                _file.Position = _file.Length;
            }
        }
    }
}
