using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts
{
    public static class DictionaryExt
    {
        public static TValue GetOrAdd<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, Func<TValue> addFunc)
        {
            if (dict.TryGetValue(key, out var existingValue))
            {
                return existingValue;
            }
            else
            {
                var added = addFunc();
                dict.Add(key, added);
                return added;
            }
        }

        public static void AddOrUpdate<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, Func<TValue> addFunc, Action<TValue> updateAction)
        {
            if (dict.TryGetValue(key, out var existingValue))
            {
                updateAction(existingValue);
            }
            else
            {
                var added = addFunc();
                dict.Add(key, added);
            }
        }
    }
}
