using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class DataFile
    {
        private const int ChunkSize = 1024;

        private readonly FileStream _buffer;
        private readonly FileStream _main;

        private int _docsInCurrentChunk = 0;

        public DataFile(string dataDir)
        {
            var bufferPath = Path.Combine(dataDir, "buffer.dat");
            var mainPath = Path.Combine(dataDir, "main.dat");

            _buffer = new FileStream(bufferPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _main = new FileStream(mainPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
        }

        public long AddDocumentAndGetChunkOffset(Document d)
        {
            if (_docsInCurrentChunk == ChunkSize)
            {
                FlushBuffer();
            }

            var docStr = JsonConvert.SerializeObject(d);
            var docBytes = Encoding.UTF8.GetBytes(docStr);

            _buffer.Write(docBytes, 0, docBytes.Length);

            return _main.Position;
        }

        private void FlushBuffer()
        {
            if (_buffer.Length == 0)
            {
                return;
            }

            
        }
    }
}
