using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class FieldIndex : IDisposable
    {
        private readonly int MaxNumberOfTermsInOneIndexFile = 1000;

        private readonly Dictionary<string, HashSet<long>> _inMemoryIx = new Dictionary<string, HashSet<long>>();
        private readonly string _indexDir;
        private readonly List<FileStream> _indexFiles;

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
                () => new HashSet<long>(),
                (postingList) => postingList.Add(dataFileOffset));

            if (_inMemoryIx.Count >= MaxNumberOfTermsInOneIndexFile)
            {
                await FlushToIndexFile();
            }
        }

        public void Dispose()
        {
            _indexFiles.ForEach(ixf => ixf.Dispose());
        }

        public IEnumerable<long> Search(SearchQuery query)
        {
            throw new NotImplementedException();
        }

        private async Task FlushToIndexFile()
        {
            var currentIxFile = _indexFiles[_indexFiles.Count - 1];

            foreach (var termWithPostingList in _inMemoryIx.OrderBy(x => x.Key))
            {
                var term = termWithPostingList.Key;
                var postingList = termWithPostingList.Value;

                await currentIxFile.WriteInt(term.Length);
                await currentIxFile.WriteString(term);
                await currentIxFile.WriteInt(postingList.Count);
                
                foreach (var entry in postingList)
                {
                    await currentIxFile.WriteLong(entry);
                }
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

        private IEnumerable<string> GetIndexFilePaths()
        {
            // TODO: check index integrity!

            var nextFileId = 1;

            var existingFilePaths = Directory.EnumerateFiles(_indexDir, "*.fix").ToList();
            if (existingFilePaths.Count != 0)
            {
                nextFileId = existingFilePaths
                    .Select(f => Path.GetFileNameWithoutExtension(f))
                    .Select(fname => int.Parse(fname))
                    .Max() + 1;
            }
            
            foreach (var existingFilePath in existingFilePaths)
            {
                yield return existingFilePath;
            }

            yield return $"{nextFileId}.fix";
        }
    }
}
