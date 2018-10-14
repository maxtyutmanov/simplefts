using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SimpleFts
{
    public class FieldIndex
    {
        private readonly Dictionary<string, HashSet<long>> _index = new Dictionary<string, HashSet<long>>();

        public FieldIndex(string fieldName)
        {

        }

        public async Task AddTerm(string term, long dataFileOffset)
        {
            _index.AddOrUpdate(
                term,
                () => new HashSet<long>(),
                (postingList) => postingList.Add(dataFileOffset));
        }

        public IEnumerable<long> Search(SearchQuery query)
        {
            throw new NotImplementedException();
        }
    }
}
