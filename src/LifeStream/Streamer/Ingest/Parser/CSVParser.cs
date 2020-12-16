using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.StreamProcessing;

namespace Streamer.Ingest.Parser
{
    using signal_t = StreamEvent<Signal>;

    public class CSVFileParser : SignalParser<signal_t>
    {
        private string filename;
        private Func<string, long> ts_decoder;
        private Func<string, float> val_decoder;

        public CSVFileParser(string filename, string signal, string devid, long t_start, long t_end,
            Func<string, long> ts_decoder,
            Func<string, float> val_decoder)
            : base(signal, devid, t_start, t_end)
        {
            this.filename = filename;
            this.ts_decoder = ts_decoder;
            this.val_decoder = val_decoder;
        }

        public CSVFileParser(string filename, string signal, string devid, long t_start, long t_end)
            : this(filename, signal, devid, t_start, t_end, long.Parse, float.Parse)
        {
        }

        public override IEnumerable<signal_t> Parse()
        {
            using var reader = new StreamReader(filename);
            var signals = new List<signal_t>();

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (line != null)
                {
                    var values = line.Split(',');
                    var t = ts_decoder(values[0]);
                    var v = val_decoder(values[1]);
                    var sig = new Signal(t, v);
                    signals.Add(StreamEvent.CreateStart(t, sig));
                }
            }

            return signals;
        }
    }

    public class CSVFileNameParser : FileNameParser<CSVFileParser>
    {
        public CSVFileNameParser(string root, string filename) : base(root, filename)
        {
        }

        protected override CSVFileParser GetSignalParser(string signal, string devid, long t_start, long t_end)
        {
            return new CSVFileParser(filename, signal, devid, t_start, t_end);
        }
    }

    public class CSVSignal : RetroSignal<CSVFileParser>
    {
        public CSVSignal(string root, List<string> files) : base(root, files)
        {
        }

        public CSVSignal(string root, string prefix, string signal, string devid = "*")
            : base(root, prefix, "csv", signal, devid)
        {
        }

        protected override EventParser<CSVFileParser> Build(string root, string filename)
        {
            return new CSVFileNameParser(root, filename);
        }
    }
}