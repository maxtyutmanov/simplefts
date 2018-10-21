using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class FieldIndex : IDisposable
    {
        // We don't bother with exact calculation of the overhead required to store another long value in the hashset.
        // Here we assume it is size of long value multiplied by some factor.
        private const int PostingListEntrySizeScore = sizeof(long) * 4;
        private readonly int MaxInMemoryIndexSizeScore = 100 * 1024 * 1024;

        private readonly Dictionary<string, HashSet<long>> _inMemoryIx = new Dictionary<string, HashSet<long>>();
        private readonly string _indexDir;
        private readonly List<FileStream> _indexFiles;
        private long _currentInMemoryIndexSizeScore = 0;

        public FieldIndex(string globalIndexDir, string fieldName)
        {
            _indexDir = Path.Combine(globalIndexDir, fieldName);
            Directory.CreateDirectory(_indexDir);
            _indexFiles = OpenIndexFilesForWrite();
        }

        public async Task AddTerm(string term, long dataFileOffset)
        {
            _inMemoryIx.AddOrUpdate(
                term,
                () =>
                {
                    // we roughly estimate the memory required to store a string as number of its characters multiplied by 2
                    _currentInMemoryIndexSizeScore += term.Length * 2 + PostingListEntrySizeScore;
                    return new HashSet<long>() { dataFileOffset };
                },
                (postingList) =>
                {
                    if (postingList.Add(dataFileOffset))
                    {
                        _currentInMemoryIndexSizeScore += PostingListEntrySizeScore;
                    }
                });

            if (_currentInMemoryIndexSizeScore >= MaxInMemoryIndexSizeScore)
            {
                await Commit();
            }
        }

        public void Dispose()
        {
            _indexFiles.ForEach(ixf => ixf.Dispose());
        }

        public IEnumerable<long> Search(string term)
        {
            foreach (var indexFilePath in GetIndexFilePaths())
            {
                foreach (var postingListEntry in SearchIndexFile(term, indexFilePath))
                {
                    yield return postingListEntry;
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

        public async Task Commit()
        {
            /*
             * Index file structure:
             * 
             * <number of terms in the index file>
             * <file offset (in bytes) of term 2> <length of term 1 representation (in bytes)> <term 1 UTF-8 bytes> <number of items in the posting list 1> <item 1 of the posting list 1> ... <item N of the posting list 1>
             * <file offset (in bytes) of term 3> <length of term 2 representation (in bytes)> <term 2 UTF-8 bytes> <number of items in the posting list 2> <item 2 of the posting list 2> ... <item N of the posting list 2>
             * ...
             * (this goes on for all terms in the index)
             * <end-of-index marker>
             */

            var newIxFile = OpenNextIndexFileForWrite();

            try
            {
                foreach (var termWithPostingList in _inMemoryIx.OrderBy(x => x.Key))
                {
                    await WriteTermAndPostingList(newIxFile, termWithPostingList.Key, termWithPostingList.Value);
                }

                // termination sign
                await newIxFile.FlushAsync();
                _indexFiles.Add(newIxFile);

                _inMemoryIx.Clear();
                _currentInMemoryIndexSizeScore = 0;
            }
            catch (Exception)
            {
                // TODO: delete ix file

                newIxFile?.Dispose();
                throw;
            }
        }

        private async Task WriteTermAndPostingList(Stream ixStream, string term, HashSet<long> postingList)
        {
            var termBytes = Encoding.UTF8.GetBytes(term);
            var lengthOfTermRecordInBytes = sizeof(long) + sizeof(int) + termBytes.Length + sizeof(int) + (postingList.Count * sizeof(long));
            var offsetOfNextTerm = ixStream.Position + lengthOfTermRecordInBytes;

            await ixStream.WriteLongAsync(offsetOfNextTerm);
            await ixStream.WriteIntAsync(termBytes.Length);
            await ixStream.WriteAsync(termBytes, 0, termBytes.Length);
            await ixStream.WriteIntAsync(postingList.Count);
            
            foreach (var entry in postingList.OrderBy(x => x))
            {
                await ixStream.WriteLongAsync(entry);
            }
        }

        private List<FileStream> OpenIndexFilesForWrite()
        {
            var result = new List<FileStream>();

            try
            {
                foreach (var path in GetIndexFilePaths())
                {
                    var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
                    result.Add(fs);
                }
            }
            catch (Exception)
            {
                foreach (var fs in result)
                {
                    fs.Dispose();
                }
                throw;
            }

            return result;
        }

        // TODO: check index integrity

        private List<string> GetIndexFilePaths() => Directory.EnumerateFiles(_indexDir, "*.fix").ToList();

        private FileStream OpenNextIndexFileForWrite()
        {
            var newIxFs = new FileStream(GetNextIndexFilePath(), FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
            return newIxFs;
        }
        
        private string GetNextIndexFilePath()
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

            return Path.Combine(_indexDir, $"{nextFileId}.fix");
        }
    }
}
