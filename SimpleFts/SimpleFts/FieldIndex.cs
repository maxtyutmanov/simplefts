using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using InMemoryIndex = System.Collections.Concurrent.ConcurrentDictionary<string, SimpleFts.Utils.ConcurrentHashSet<long>>;
using SimpleFts.Utils;

namespace SimpleFts
{
    public class FieldIndex
    {
        // We don't bother with exact calculation of the overhead required to store another long value in the hashset.
        // Here we assume it is the size of long value (8 bytes) multiplied by some factor.
        private const int PostingListEntrySizeScore = sizeof(long) * 8;
        private readonly int MemoryPressureScoreThreshold = 100 * 1024 * 1024;

        private readonly SemaphoreSlim _commitLock = new SemaphoreSlim(1, 1);

        private InMemoryIndex _inMemoryIx = new InMemoryIndex();
        private readonly string _indexDir;
        private long _memoryPressureScore = 0;

        public FieldIndex(string rootIndexDir, string fieldName)
        {
            _indexDir = Path.Combine(rootIndexDir, fieldName);
            Directory.CreateDirectory(_indexDir);
        }

        public async Task AddTerm(string term, long dataFileOffset)
        {
            _inMemoryIx.AddOrUpdate(
                term,
                (key) =>
                {
                    // we roughly estimate the memory required to store a string as number of its characters multiplied by 2
                    Interlocked.Add(ref _memoryPressureScore, term.Length * 2 + PostingListEntrySizeScore);

                    var hs = new ConcurrentHashSet<long>();
                    hs.Add(dataFileOffset);
                    return hs;
                },
                (key, postingList) =>
                {
                    if (postingList.Add(dataFileOffset))
                    {
                        Interlocked.Add(ref _memoryPressureScore, PostingListEntrySizeScore);
                    }
                    return postingList;
                });

            await CommitIfHighMemoryPressure();
        }

        public IEnumerable<long> Search(string term)
        {
            // multiple inverted index entries may point to the same chunks in datafile, hence the deduplication
            var seenEntries = new HashSet<long>();

            foreach (var indexFilePath in GetIndexFilePaths())
            {
                foreach (var postingListEntry in SearchIndexFile(term, indexFilePath))
                {
                    if (!seenEntries.Contains(postingListEntry))
                    {
                        yield return postingListEntry;
                        seenEntries.Add(postingListEntry);
                    }
                }
            }
        }

        public async Task Commit()
        {
            await _commitLock.WaitAsync();

            try
            {
                await CommitInternal();
            }
            finally
            {
                _commitLock.Release();
            }
        }

        private async Task CommitInternal()
        {
            // detaching old in memory index so that all new updates will go to the new 
            // in memory index while the old one is being committed to disk
            var newInMemoryIx = new InMemoryIndex();
            var oldInMemoryIx = Interlocked.Exchange(ref _inMemoryIx, newInMemoryIx);
            Interlocked.Exchange(ref _memoryPressureScore, 0);

            if (oldInMemoryIx.IsEmpty)
            {
                // nothing to commit
                return;
            }

            /*
             * Index file structure:
             * 
             * <file offset (in bytes) of term 2> <length of term 1 representation (in bytes)> <term 1 UTF-8 bytes> <number of items in the posting list 1> <item 1 of the posting list 1> ... <item N of the posting list 1>
             * <file offset (in bytes) of term 3> <length of term 2 representation (in bytes)> <term 2 UTF-8 bytes> <number of items in the posting list 2> <item 2 of the posting list 2> ... <item N of the posting list 2>
             * ...
             * (this goes on for all terms in the index)
             * <end-of-index marker>
             */

            using (var newIxFile = OpenNextIndexFileForWrite())
            {
                foreach (var termWithPostingList in oldInMemoryIx.OrderBy(x => x.Key))
                {
                    await WriteTermAndPostingList(newIxFile, termWithPostingList.Key, termWithPostingList.Value);
                }

                await newIxFile.FlushAsync();
            }
        }

        private async Task CommitIfHighMemoryPressure()
        {
            if (_memoryPressureScore >= MemoryPressureScoreThreshold)
            {
                await _commitLock.WaitAsync();

                try
                {
                    // double check - what if some other thread has executed (or is executing) the commit already
                    if (_memoryPressureScore >= MemoryPressureScoreThreshold)
                    {
                        await CommitInternal();
                    }
                }
                finally
                {
                    _commitLock.Release();
                }
            }
        }

        private IEnumerable<long> SearchIndexFile(string targetTerm, string indexFilePath)
        {
            var comparer = StringComparer.OrdinalIgnoreCase;

            using (var ixFile = new FileStream(indexFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                while (ixFile.NotEof())
                {
                    var nextTermOffset = ixFile.ReadLong();

                    var termLengthInBytes = ixFile.ReadInt();
                    var term = ixFile.ReadUtf8String(termLengthInBytes);
                    var cmpResult = comparer.Compare(term, targetTerm);

                    if (cmpResult > 0)
                    {
                        // We've got past the place where the target term could have been in the index
                        // (we conclude this because terms are sorted alphabetically)
                        break;
                    }
                    else if (cmpResult < 0)
                    {
                        ixFile.Position = nextTermOffset;
                        continue;
                    }
                    else
                    {
                        var postingListLength = ixFile.ReadInt();

                        for (int i = 0; i < postingListLength; i++)
                        {
                            var entry = ixFile.ReadLong();
                            yield return entry;
                        }

                        break;
                    }
                }
            }
        }

        private async Task WriteTermAndPostingList(Stream ixStream, string term, ConcurrentHashSet<long> postingList)
        {
            var termBytes = Encoding.UTF8.GetBytes(term);
            // <offset of next term> + <# of term bytes> + <term bytes array> + <# of posting list items> + <posting list items>
            var lengthOfTermRecordInBytes = sizeof(long) + sizeof(int) + termBytes.Length + sizeof(int) + (postingList.Count * sizeof(long));
            var offsetOfNextTerm = ixStream.Position + lengthOfTermRecordInBytes;

            await ixStream.WriteLongAsync(offsetOfNextTerm);
            await ixStream.WriteIntAsync(termBytes.Length);
            await ixStream.WriteAsync(termBytes, 0, termBytes.Length);
            await ixStream.WriteIntAsync(postingList.Count);
            
            foreach (var entry in postingList.GetItems().OrderBy(x => x))
            {
                await ixStream.WriteLongAsync(entry);
            }
        }

        // TODO: check index integrity

        private FileStream OpenNextIndexFileForWrite()
        {
            var nextFileId = 1;

            var existingFilePaths = GetIndexFilePaths();
            if (existingFilePaths.Count != 0)
            {
                nextFileId = existingFilePaths
                    .Select(f => Path.GetFileNameWithoutExtension(f))
                    .Select(fname => int.Parse(fname))
                    .Max() + 1;
            }

            var nextIxPath = Path.Combine(_indexDir, $"{nextFileId}.fix");

            var newIxFs = new FileStream(nextIxPath, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
            return newIxFs;
        }

        private List<string> GetIndexFilePaths() => Directory.EnumerateFiles(_indexDir, "*.fix").ToList();
    }
}
