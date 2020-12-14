using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;

namespace LifeStream
{
    class Program
    {
        static void NonFuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform, bool print)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            long count = 0;
            s_obs
                .ToStreamEventObservable()
                .Where(e => e.IsData && (e.IsInterval || e.IsStart))
                .ForEach(e =>
                {
                    count++;
                    if (print)
                    {
                        Console.WriteLine("{0}", e);
                    }
                });
            sw.Stop();
            Console.WriteLine("Count: {0} events, Time: {1} sec, Throughput: {2:#.###} million events/sec",
                count, sw.Elapsed.TotalSeconds, count / (sw.Elapsed.TotalSeconds * Math.Pow(10, 6)));
        }

        static void FuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<FOperation<Signal>, FOperation<TResult>> transform, bool print)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var fStart = stream
                    .FuseStart()
                ;

            var s_obs = transform(fStart.GetFOP())
                    .FuseEnd()
                ;

            fStart.Connect();
            long count = 0;
            s_obs
                .ToStreamEventObservable()
                .Where(e => e.IsData && (e.IsInterval || e.IsStart))
                .ForEach(e =>
                {
                    count++;
                    if (print)
                    {
                        Console.WriteLine("{0}", e);
                    }
                });
            sw.Stop();
            Console.WriteLine("Count: {0} events, Time: {1} sec, Throughput: {2:#.###} million events/sec",
                count, sw.Elapsed.TotalSeconds, count / (sw.Elapsed.TotalSeconds * Math.Pow(10, 6)));
        }

        static void MultiNonFuseTest(
            Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data, bool print)
        {
            var abp = abp_data();
            var ecg = ecg_data();

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
            long count = 0;
            s_obs
                .ToStreamEventObservable()
                .Where(e => e.IsData && (e.IsInterval || e.IsStart))
                .ForEach(e =>
                {
                    count++;
                    if (print)
                    {
                        Console.WriteLine("{0}", e);
                    }
                });
            sw.Stop();
            Console.WriteLine("Count: {0} events, Time: {1} sec, Throughput: {2:#.###} million events/sec",
                count, sw.Elapsed.TotalSeconds, count / (sw.Elapsed.TotalSeconds * Math.Pow(10, 6)));
        }

        internal static void PairJoiner(Signal l, Signal r, out FStreamable.SigPair o)
        {
            o.s = l;
            o.e = r;
        }

        static void MultiFuseTest(Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data,
            bool print)
        {
            var abp = abp_data();
            var ecg = ecg_data();

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
            long count = 0;
            s_obs
                .ToStreamEventObservable()
                .Where(e => e.IsData && (e.IsInterval || e.IsStart))
                .ForEach(e =>
                {
                    count++;
                    if (print)
                    {
                        Console.WriteLine("{0}", e);
                    }
                });
            sw.Stop();
            Console.WriteLine("Count: {0} events, Time: {1} sec, Throughput: {2:#.###} million events/sec",
                count, sw.Elapsed.TotalSeconds, count / (sw.Elapsed.TotalSeconds * Math.Pow(10, 6)));
        }

        static void FillJoiner<T>(T l, T r, out T o) => o = l;

        static void FillConstSelector(long t, Signal s, out Signal o)
        {
            if (t == s.ts)
            {
                o = s;
            }
            else
            {
                o.ts = t;
                o.val = 0;
            }
        }

        static void Main(string[] args)
        {
            Config.DataBatchSize = 600000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.ForceRowBasedExecution = true;

            int duration = Int32.Parse(args[0]);
            var testcase = args[1].ToLower();
            var engine = args[2].ToLower();
            bool print = (args.Length == 4 && args[3].Equals("dbg"));
            Console.Write("Benchmark: {0}, Engine: {1}, ", testcase, engine);

            const int start = 0;
            const int freq = 500;
            const int period = 1000 / freq;
            const long window = 60000;
            const long gap_tol = window;
            Config.DataGranularity = window;

            Func<IStreamable<Empty, Signal>> data = () =>
            {
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };

            Func<IStreamable<Empty, Signal>> abp_data = () =>
            {
                const int freq = 125;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };

            Func<IStreamable<Empty, Signal>> ecg_data = () =>
            {
                const int freq = 500;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };

            switch (testcase + "_" + engine)
            {
                case "normalize_trill":
                    NonFuseTest(data, stream =>
                            stream
                                .Normalize(window),
                        print);
                    break;
                case "normalize_lifestream":
                    FuseTest(data, stream =>
                            stream
                                .Normalize(period, window),
                        print);
                    break;
                case "passfilter_trill":
                    NonFuseTest(data, stream =>
                            stream
                                .BandPassFilter(period, window, 40, 300),
                        print);
                    break;
                case "passfilter_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    FuseTest(data, stream =>
                            stream
                                .BandPassFilter(period, window, 40, 300),
                        print);
                    break;
                case "fillconst_trill":
                    NonFuseTest(data, stream =>
                            stream
                                .FillConst(period, gap_tol, 0),
                        print);
                    break;
                case "fillconst_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    FuseTest(data, stream =>
                            stream
                                .FillConst(period, gap_tol, 0),
                        print);
                    break;
                case "fillmean_trill":
                    NonFuseTest(data, stream =>
                            stream
                                .FillMean(window, period, gap_tol),
                        print);
                    break;
                case "fillmean_lifestream":
                    FuseTest(data, stream =>
                            stream
                                .FillMean(window, period, gap_tol),
                        print);
                    break;
                case "resample_trill":
                    NonFuseTest(data, stream =>
                            stream
                                .Resample(period, period / 2),
                        print);
                    break;
                case "resample_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    FuseTest(data, stream =>
                            stream
                                .Resample(period, period / 2, period),
                        print);
                    break;
                case "endtoend_trill":
                    MultiNonFuseTest(abp_data, ecg_data, print);
                    break;
                case "endtoend_lifestream":
                    MultiFuseTest(abp_data, ecg_data, print);
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0} on {1}", testcase, engine);
                    break;
            }

            Config.StreamScheduler.Stop();
        }
    }
}