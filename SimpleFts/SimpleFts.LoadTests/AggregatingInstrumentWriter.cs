using SimpleFts.Core.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SimpleFts.LoadTests
{
    public class AggregatingInstrumentWriter : IInstrumentationWriter
    {
        private readonly ConcurrentDictionary<string, long> _opStats = new ConcurrentDictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        public void Write(string opName, long elapsedTicks)
        {
            _opStats.AddOrUpdate(opName, elapsedTicks, (existingOpName, existingVal) => existingVal + elapsedTicks);
        }

        public void WriteStats(TextWriter writer)
        {
            foreach (var pair in _opStats)
            {
                writer.WriteLine($"[Instrumentation] {pair.Key}: {TimeSpan.FromTicks(pair.Value)}");
            }
        }
    }
}
