using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts.Core.Utils
{
    public interface IInstrumentationWriter
    {
        void Write(string opName, long elapsedTicks);
    }
}
