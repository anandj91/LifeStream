using System.Collections.Generic;
using System.IO;
using System.Reactive;
using Microsoft.StreamProcessing;

namespace Streamer.Ingest.Parser
{
    using index_t = PartitionedStreamEvent<string, Unit>;

    public class IndexParser : EventParser<index_t>
    {
        private string filename;

        public IndexParser(string filename)
        {
            this.filename = filename;
        }

        public IEnumerable<index_t> Parse()
        {
            using var reader = new StreamReader(filename);
            var index = new List<index_t>();

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (line != null)
                {
                    var values = line.Split(',');
                    var devid = values[1].Trim();
                    var s = long.Parse(values[2]);
                    var e = long.Parse(values[3]);
                    index.Add(PartitionedStreamEvent.CreateInterval(devid, s, e, Unit.Default));
                }
            }

            return index;
        }
    }
}