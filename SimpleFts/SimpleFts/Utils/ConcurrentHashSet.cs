using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Utils
{
    public class ConcurrentHashSet<T>
        where T : IEquatable<T>
    {
        private readonly ConcurrentDictionary<T, bool> _internalHs = new ConcurrentDictionary<T, bool>();

        public int Count => _internalHs.Count;

        public IEnumerable<T> GetItems()
        {
            return _internalHs.Keys;
        }

        public bool Add(T item)
        {
            return _internalHs.AddOrUpdate(item, true, (existingItem, _) => false);
        }
    }
}
