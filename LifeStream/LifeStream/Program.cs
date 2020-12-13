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
        static void NonFuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data();

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

        static void FuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<FOperation<Signal>, FOperation<TResult>> transform)
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
            var count = s_obs
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .Count()
                    .Wait()
                ;
            sw.Stop();
            Console.WriteLine("Count: {0}, Time: {1}", count, sw.Elapsed.TotalSeconds);
        }

        static void MultiNonFuseTest(
            Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data)
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

        static void MultiFuseTest(Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data)
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

            var testcase = args[0].ToLower();
            var engine = args[1].ToLower();

            const int start = 0;
            const int duration = 60000;
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
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };
            
            Func<IStreamable<Empty, Signal>> ecg_data = () =>
            {
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
                            .Normalize(window)
                    );
                    break;
                case "normalize_lifestream":
                    FuseTest(data, stream =>
                        stream
                            .Normalize(500, window)
                    );
                    break;
                case "passfilter_trill":
                    NonFuseTest(data, stream =>
                        stream
                            .BandPassFilter()
                    );
                    break;
                case "passfilter_lifestream":
                    FuseTest(data, stream =>
                        stream
                            .Normalize(500, window)
                    );
                    break;
                case "fillconst_trill":
                    NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "fillconst_lifestream":
                    FuseTest(data, stream =>
                        stream
                            .Normalize(500, window)
                    );
                    break;
                case "fillmean_trill":
                    NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "fillmean_lifestream":
                    FuseTest(data, stream =>
                        stream
                            .Normalize(500, window)
                    );
                    break;
                case "resample_trill":
                    NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "resample_lifestream":
                    FuseTest(data, stream =>
                        stream
                            .Normalize(500, window)
                    );
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0} on {1}", testcase, engine);
                    break;
            }

            Config.StreamScheduler.Stop();
        }
    }
}