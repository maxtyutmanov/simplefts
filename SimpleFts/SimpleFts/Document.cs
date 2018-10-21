using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleFts
{
    public class Document
    {
        public Dictionary<string, string> Fields { get; } = new Dictionary<string, string>();

        public void SetField(string fieldName, string fieldValue)
        {
            Fields[fieldName] = fieldValue;
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }
    }
}
