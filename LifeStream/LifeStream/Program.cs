using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;
using Streamer.Ingest.Parser;

namespace LifeStream
{
    class Program
    {
        static void NonFuseTest<TResult>(Func<int, IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data(1000);

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            var count = s_obs
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .Count()
                    .Wait()
                ;
            sw.Stop();
            Console.WriteLine("Count: {0}, Time: {1}", count, sw.Elapsed.TotalSeconds);
        }

        static void FuseTest<TResult>(Func<int, IStreamable<Empty, Signal>> data,
            Func<FOperation<Signal>, FOperation<TResult>> transform)
        {
            var stream = data(1000);

            var sw = new Stopwatch();
            sw.Start();
            var fStart = stream
                    .FuseStart()
                ;

            var s_obs = transform(fStart.GetFOP())
                    .FuseEnd()
                ;

            fStart.Connect();
            var count = s_obs
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .Count()
                    .Wait()
                ;
            sw.Stop();
            Console.WriteLine("Count: {0}, Time: {1}", count, sw.Elapsed.TotalSeconds);
        }

        static void MultiNonFuseTest(Func<int, IStreamable<Empty, Signal>> data)
        {
            var abp = data(125);
            var ecg = data(500);

            var sw = new Stopwatch();
            sw.Start();
            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = abp
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2)
                    .Normalize(window)
                ;
            var fstream2 = ecg
                    .FillMean(window, 2, gap_tol)
                    .Normalize(window)
                ;

            var s_obs = fstream2
                    .Join(fstream1, (l, r) => new {l, r})
                ;
            var count = s_obs
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .Count()
                    .Wait()
                ;
            sw.Stop();
            Console.WriteLine("Count: {0}, Time: {1}", count, sw.Elapsed.TotalSeconds);
        }

        internal static void PairJoiner(Signal l, Signal r, out FStreamable.SigPair o)
        {
            o.s = l;
            o.e = r;
        }

        static void MultiFuseTest(Func<int, IStreamable<Empty, Signal>> data)
        {
            var abp = data(125);
            var ecg = data(500);

            var sw = new Stopwatch();
            sw.Start();
            var fabp = abp
                    .FuseStart()
                ;
            var fecg = ecg
                    .FuseStart()
                ;

            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = fabp.GetFOP()
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2, 8)
                    .Normalize(2, window)
                ;
            var fstream2 = fecg.GetFOP()
                    .FillMean(window, 2, gap_tol)
                    .Normalize(2, window)
                ;

            var s_obs = fstream2
                    .Join<Signal, Signal, FStreamable.SigPair>(fstream1, PairJoiner)
                    .FuseEnd()
                ;

            fabp.Connect();
            fecg.Connect();
            var count = s_obs
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .Count()
                    .Wait()
                ;
            sw.Stop();
            Console.WriteLine("Count: {0}, Time: {1}", count, sw.Elapsed.TotalSeconds);
        }

        static void Main(string[] args)
        {
            Config.DataBatchSize = 600000;
            Config.FuseFactor = 1;
            //Config.FuseFactor = 60000/2;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(1);
            Config.ForceRowBasedExecution = true;

            const int start = 0;
            const int duration = 60000;
            const int freq = 500;
            const long period = 1000 / freq;
            const long window = 60000;
            const long gap_tol = window;
            Config.DataGranularity = window;

            Func<int, IStreamable<Empty, Signal>> data = (int freq) =>
            {
                string loc = "2018/01/";
                //var obs = new TestObs("test", start, duration, freq);
                /*
                var abp = new CSVSignal(
                            "/home/anand/stream/Streamer/Resources/csv/",
                            loc,
                            "MDC_PRESS_BLD_ART_ABP",
                            "3-73_2"
                        )
                        .Parse()
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + 8)
                    ;
                    */
                var ecg = new TestObs()
                        .ToTemporalStreamable(e => e.ts, e => e.ts + 2)
                    ;
                var ret = ecg
                        .Cache()
                    ;
                return ret
                    ;
            };
            /*
            NonFuseTest(data, false,
                bs => bs
                    //.Select(e => Unit.Default)
                    //.Normalize(window)
                    //.Stitch()
                    //.Chop(0, 1)
                    //.Multicast(s => s.ClipEventDuration(s))
                    //.Select(e =>e.val)
                    //.Where(e => e.val%2 == 0)
                    //.TumblingWindowLifetime(window)
                    //.Aggregate(w => w.Average(e=>e.val))
                    //.Multicast(s =>s.Join(s, (l, r) => new{l, r}))
                    //.ConsecutivePairs((l, r) => new {l, r})
                    //.Normalize(window)
                    //.FillConst(period, gap_tol, 0)
                    .FillMean(window, period, gap_tol)
                    .Resample(period, 1)
                    .Normalize(window)
                    .BandPassFilter(period, window, 2, 300)
            );
            void Selector(long t, Signal p, out float o)
            {
                o = p.val;
            }
            FuseTest(data, false,
                fop => fop
                    //.Chop(1)
                    //.ClipEventDuration()
                    //.Select<Signal, float>(Selector)
                    //.Where(e => e.val%2 == 0)
                    //.ConsecutivePairs<Signal, FStreamable.SigPair>(PairJoiner)
                    //.Multicast(s =>s.Join<Signal, Signal, FStreamable.SigPair>(s, PairJoiner))
                    //.Aggregate(w => w.Average(e=>e.val), window, window)
                    //.Normalize(period, window)
                    //.FillConst(period, gap_tol, 0)
                    .FillMean(window, period, gap_tol)
                    .Resample(period, 1, period)
                    .Normalize(1, window)
                    .BandPassFilter(1, window, 2, 300)
            );
                        */
            string loc = "2018/01/31/";
            //MultiNonFuseTest(loc, false);
            //MultiFuseTest(loc, false);
            Config.StreamScheduler.Stop();
        }
    }
}