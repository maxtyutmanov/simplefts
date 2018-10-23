﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SimpleFts.LoadTests
{
    class Program
    {
        const string RootIndexDir = @"E:\work\simplefts_store\index_root";
        const string DataDir = @"E:\work\simplefts_store\datadir";
        const int TestSetSize = 5000;

        static Random _rand = new Random();

        static async Task Main(string[] args)
        {
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

                counter = 0;
                sw.Restart();
                foreach (var id in Enumerable.Range(1, TestSetSize))
                {
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
                        Console.WriteLine("Performed {0} searched in the database in {1}", counter, sw.Elapsed);
                    }
                }

                Console.WriteLine("Finished all searches in {0}", sw.Elapsed);
            }

            Console.ReadLine();
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