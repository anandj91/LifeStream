using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Streamer.Ingest.Parser
{
    public interface EventParser<T>
    {
        public IEnumerable<T> Parse();
    }

    public abstract class SignalParser<T> : EventParser<T>
    {
        public string signal;
        public string devid;
        public long t_start;
        public long t_end;

        protected SignalParser(string signal, string devid, long t_start, long t_end)
        {
            this.signal = signal;
            this.devid = devid;
            this.t_start = t_start;
            this.t_end = t_end;
        }

        public abstract IEnumerable<T> Parse();
    }

    public abstract class FileNameParser<T> : EventParser<T>
    {
        public string root;
        public string filename;

        protected FileNameParser(string root, string filename)
        {
            this.root = root;
            this.filename = filename;
        }

        public IEnumerable<T> Parse()
        {
            var list = new List<T>();

            string path = filename.Substring(root.Length);
            string[] dates = path.Split(@"/");
            var t_start = new DateTimeOffset(new DateTime(int.Parse(dates[0]), int.Parse(dates[1]),
                int.Parse(dates[2]), int.Parse(dates[3]), 0, 0)).ToUnixTimeMilliseconds();
            var t_end = t_start + 3600000;
            var fname = dates[^1];
            var devid = fname.Split("~")[0];
            var signal = fname.Split("~")[1].Split(".")[0];

            list.Add(GetSignalParser(signal, devid, t_start, t_end));

            return list;
        }

        protected abstract T GetSignalParser(string signal, string devid, long t_start, long t_end);
    }

    public abstract class RetroSignal<T> : IObservable<T>
    {
        private string root;
        private List<string> files;

        public RetroSignal(string root, List<string> files)
        {
            this.root = root;
            this.files = files;
        }

        public RetroSignal(string root, string prefix, string ext, string signal, string devid = "*")
            : this(root,
                new List<string>(Directory.GetFiles(root + prefix, devid + "~" + signal + "." + ext,
                    SearchOption.AllDirectories)))
        {
            this.files.Sort();
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            return this.files
                .Select(e => Build(root, e))
                .ToObservable()
                .Parse()
                .Subscribe(observer);
        }

        protected abstract EventParser<T> Build(string root, string filename);
    }
}