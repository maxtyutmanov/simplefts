using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts.Core.Serialization
{
    public static class DocumentSerializer
    {
        public static async Task SerializeBatch(IReadOnlyCollection<Document> docsBatch, Stream stream)
        {
            var batchStartOffset = stream.Position;

            await stream.WriteIntAsync(docsBatch.Count);

            foreach (var doc in docsBatch)
            {
                await SerializeOneDocument(doc, stream);
            }

            await stream.WriteLongAsync(batchStartOffset);
        }

        public static async Task<List<Document>> DeserializeBatch(Stream stream)
        {
            var docsCount = await stream.ReadIntAsync();
            var batch = new List<Document>(docsCount);

            for (int i = 0; i < docsCount; i++)
            {
                var doc = await DeserializeOneDocument(stream);
                batch.Add(doc);
            }

            stream.Position += sizeof(long);
            return batch;
        }

        public static async Task<List<Document>> DeserializeBatchFromRightToLeft(Stream stream)
        {
            stream.Position -= sizeof(long);
            var batchStartOffset = await stream.ReadLongAsync();
            stream.Position = batchStartOffset;

            var batch = await DeserializeBatch(stream);

            // since we read from the end to the beginning, it makes sense
            // to move position to the beginning of the batch after we read it
            if (batchStartOffset != 0)
            {
                stream.Position = batchStartOffset - sizeof(long);
            }

            return batch;
        }

        private static async Task SerializeOneDocument(Document doc, Stream stream)
        {
            await stream.WriteLongAsync(doc.Id);
            await stream.WriteIntAsync(doc.Fields.Count);
            
            foreach (var field in doc.Fields)
            {
                await stream.WriteUtf8StringWithLengthAsync(field.Key);
                await stream.WriteUtf8StringWithLengthAsync(field.Value);
            }
        }

        private static async Task<Document> DeserializeOneDocument(Stream stream)
        {
            var doc = new Document();
            doc.Id = await stream.ReadLongAsync();
            var fieldsCount = await stream.ReadIntAsync();

            for (int i = 0; i < fieldsCount; i++)
            {
                var fieldKey = await stream.ReadUtf8StringFromBufferWithLengthAsync();
                var fieldValue = await stream.ReadUtf8StringFromBufferWithLengthAsync();
                doc.Fields.Add(fieldKey, fieldValue);
            }

            return doc;
        }
    }
}
