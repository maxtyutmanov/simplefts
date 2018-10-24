using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace SimpleFts.Core.Utils
{
    public class Measured : IDisposable
    {
        private static IInstrumentationWriter _writer;

        private readonly Stopwatch _sw;
        private readonly string _opName;

        public Measured(string opName)
        {
            _sw = Stopwatch.StartNew();
            _opName = opName;
        }

        public void Dispose()
        {
            _sw.Stop();
            _writer?.Write(_opName, _sw.ElapsedTicks);
        }

        public static void Initialize(IInstrumentationWriter writer)
        {
            _writer = writer;
        }

        public static IDisposable Operation(string opName)
        {
            return new Measured(opName);
        }
    }
}
