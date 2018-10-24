using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Core.Utils
{
    public static class EnumerableExt
    {
        public static IEnumerable<IReadOnlyList<T>> GetByBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            var currentBatch = new List<T>();
            foreach (var item in source)
            {
                currentBatch.Add(item);
                if (currentBatch.Count == batchSize)
                {
                    yield return currentBatch;
                    currentBatch.Clear();
                }
            }

            if (currentBatch.Count != 0)
            {
                yield return currentBatch;
            }
        }
    }
}
