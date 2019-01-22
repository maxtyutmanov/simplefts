using SimpleFts.Core;
using SimpleFts.Core.Utils;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts.LoadTests
{
    class Program
    {
        const string RootIndexDir = @"E:\work\simplefts_store\index_root";
        const string DataDir = @"E:\work\simplefts_store\datadir";
        const int TestSetSize = 10000;
        const int BatchSize = 256;

        static Random _rand = new Random();

        static async Task Main(string[] args)
        {
            await SearchExistingDb();
            //await TestWithBatching();
            //await RecreateTestDb();
            Console.ReadLine();
        }

        static async Task RecreateTestDb()
        {
            ClearData();

            var instrumentWriter = new AggregatingInstrumentWriter();
            Measured.Initialize(instrumentWriter);

            using (var db = new Database(DataDir, RootIndexDir))
            {
                await WriteToDbByBatches(db);
                instrumentWriter.WriteStats(Console.Out);
            }
        }

        static async Task SearchExistingDb()
        {
            var instrumentWriter = new AggregatingInstrumentWriter();
            Measured.Initialize(instrumentWriter);

            using (var db = new Database(DataDir, RootIndexDir))
            {
                RunSearches(db);
                instrumentWriter.WriteStats(Console.Out);
            }
        }

        static async Task TestWithBatching()
        {
            ClearData();

            var instrumentWriter = new AggregatingInstrumentWriter();
            Measured.Initialize(instrumentWriter);

            using (var db = new Database(DataDir, RootIndexDir))
            {
                await WriteToDbByBatches(db);
                instrumentWriter.WriteStats(Console.Out);

                RunSearches(db);
                instrumentWriter.WriteStats(Console.Out);
            }

            Console.ReadLine();
        }

        static async Task WriteToDbByBatches(Database db)
        {
            var docs = Enumerable.Range(1, TestSetSize).Select(id => GenerateRandomDocument(id));
            var counter = 0;
            var sw = Stopwatch.StartNew();
            foreach (var docsBatch in docs.GetByBatches(BatchSize))
            {
                await db.AddDocuments(docsBatch);
                if (++counter % 10 == 0)
                {
                    Console.WriteLine("Added {0} documents to the DB in {1}", counter * BatchSize, sw.Elapsed);
                }
            }

            await db.Commit();
            Console.WriteLine("Finished adding documents in {0}", sw.Elapsed);
        }

        static void RunSearches(Database db)
        {
            var rand = new Random();
            var counter = 0;
            var sw = Stopwatch.StartNew();

            for (var i = 0; i < TestSetSize; ++i)
            {
                var id = rand.Next(1, TestSetSize + 1);

                var foundDocs = db.Search(new SearchQuery()
                {
                    Field = "id",
                    Term = id.ToString()
                }).ToArray();

                if (foundDocs.Length == 0)
                {
                    Console.WriteLine("Document with id={0} wasn't found in the DB", id);
                }

                if (foundDocs.Length > 1)
                {
                    Console.WriteLine("There are {0} duplicate documents with id {1}", foundDocs.Length, id);
                }

                if (++counter % 1000 == 0)
                {
                    Console.WriteLine("Performed {0} searches in the database in {1}", counter, sw.Elapsed);
                }
            }

            Console.WriteLine("Finished all searches in {0}", sw.Elapsed);
        }

        static async Task TestWithoutBatching()
        {
            var instrumentWriter = new AggregatingInstrumentWriter();
            Measured.Initialize(instrumentWriter);

            var docs = Enumerable.Range(1, TestSetSize).Select(id => GenerateRandomDocument(id));

            using (var db = new Database(DataDir, RootIndexDir))
            {
                var counter = 0;
                var sw = Stopwatch.StartNew();
                foreach (var doc in docs)
                {
                    await db.AddDocument(doc);
                    if (++counter % 1000 == 0)
                    {
                        Console.WriteLine("Added {0} documents to the DB in {1}", counter, sw.Elapsed);
                    }
                }

                await db.Commit();

                Console.WriteLine("Finished adding documents in {0}", sw.Elapsed);
                instrumentWriter.WriteStats(Console.Out);

                RunSearches(db);
                instrumentWriter.WriteStats(Console.Out);
            }

            Console.ReadLine();
        }

        static void ClearData()
        {
            if (Directory.Exists(RootIndexDir))
            {
                Directory.Delete(RootIndexDir, true);
            }

            if (Directory.Exists(DataDir))
            {
                Directory.Delete(DataDir, true);
            }
        }

        static Document GenerateRandomDocument(int id)
        {
            var randNum1 = _rand.Next(1000);
            var randNum2 = _rand.Next(1000);

            var doc = new Document();
            doc.Fields.Add("id", id.ToString());
            doc.Fields.Add("field1", $"{randNum1} rand value {randNum1}");
            doc.Fields.Add("field2", $"{randNum2} another rand value {randNum2}");

            if (randNum2 > 500)
            {
                doc.Fields.Add("field3", $"{randNum2 / 2} another rand value {randNum2 / 2}");
            }

            return doc;
        }
    }
}
