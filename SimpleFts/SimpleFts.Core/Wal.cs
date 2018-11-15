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
    public class Wal
    {
        private readonly FileStream _file;
        private readonly SemaphoreSlim _commitLock = new SemaphoreSlim(1, 1);

        public Wal(string walPath)
        {
            _file = new FileStream(walPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            _file.Position = _file.Length;
        }

        public async Task Commit(Tran tran)
        {
            await _commitLock.WaitAsync().ConfigureAwait(false);

            try
            {
                await DocumentSerializer.SerializeBatch(tran.Documents, _file).ConfigureAwait(false);
            }
            finally
            {
                _commitLock.Release();
            }
        }
    }
}
