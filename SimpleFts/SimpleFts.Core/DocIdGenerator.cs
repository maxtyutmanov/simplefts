using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace SimpleFts.Core
{
    public class DocIdGenerator
    {
        private long _currentId;

        public DocIdGenerator(long startId)
        {
            _currentId = startId;
        }

        public long GetNextId() => Interlocked.Increment(ref _currentId);
    }
}
