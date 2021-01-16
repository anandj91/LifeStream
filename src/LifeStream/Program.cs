using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;
using Streamer.Ingest.Parser;

namespace LifeStream
{
    class Program
    {
        static double NonFuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double FuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
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
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double MultiNonFuseTest(
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
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        internal static void PairJoiner(Signal l, Signal r, out FStreamable.SigPair o)
        {
            o.s = l;
            o.e = r;
        }

        static double MultiFuseTest(Func<IStreamable<Empty, Signal>> abp_data,
            Func<IStreamable<Empty, Signal>> ecg_data)
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
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double Linezero(Func<IStreamable<Empty, Signal>> abp_data, long slide_window, long std_window)
        {
            var abp = abp_data();

            var sw = new Stopwatch();
            sw.Start();
            var fstream1 = abp
                    .Select(e => e.val)
                    .AttachAggregate(
                        s => s,
                        w => w.JoinedAggregate(
                            w.Average(e => e),
                            w.StandardDeviation(e => e),
                            (avg, std) => new {avg, std}
                        ),
                        (signal, score) => (float) ((signal - score.avg) / score.std),
                        std_window, slide_window)
                ;

            var s_obs = fstream1
                ;
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double LinezeroFuse(Func<IStreamable<Empty, Signal>> abp_data, long slide_window, long std_window)
        {
            var abp = abp_data();

            var sw = new Stopwatch();
            sw.Start();
            var fabp = abp
                    .FuseStart()
                ;

            var fstream1 = fabp.GetFOP()
                    .Select((long ts, Signal i, out float o) => o = i.val)
                    .Multicast(s => s
                        .Join(s
                                .Aggregate(w => w.StdScore(), std_window, slide_window),
                            (float val, StdScore ss, out float o) => o = (float) ((val - ss.avg) / ss.std)
                        )
                    )
                ;

            var s_obs = fstream1
                    .FuseEnd()
                ;

            fabp.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        struct MaskedSignal
        {
            public bool mask;
            public Signal signal;

            public MaskedSignal(bool mask, Signal signal)
            {
                this.mask = mask;
                this.signal = signal;
            }
        }

        static IStreamable<Empty, MaskedSignal> Trans(IStreamable<Empty, Signal> source)
        {
            var window = 60000;
            var iperiod = 8;
            var operiod = 2;
            var gap_tol = 60000;
            return source
                .Multicast(s => s
                    .Normalize(window)
                    .Resample(iperiod, operiod)
                    .Chop(0, operiod, gap_tol)
                    .Join(s.Mask(operiod, gap_tol),
                        (s, b) => new MaskedSignal(b, s)
                    )
                );
        }

        static double CAP(Func<IStreamable<Empty, Signal>> pulse_data,
            Func<IStreamable<Empty, Signal>> hr_data,
            Func<IStreamable<Empty, Signal>> resp_data,
            Func<IStreamable<Empty, Signal>> spo2_data,
            Func<IStreamable<Empty, Signal>> etco2_data,
            Func<IStreamable<Empty, Signal>> abpm_data)
        {
            var pulse = Trans(pulse_data());
            var hr = Trans(hr_data());
            var resp = Trans(resp_data());
            var spo2 = Trans(spo2_data());
            var etco2 = Trans(etco2_data());
            var abpm = Trans(abpm_data());

            var sw = new Stopwatch();
            sw.Start();

            var pulse_hr = pulse.Join(hr, (p, h) => new {p, h});
            var resp_spo2 = resp.Join(spo2, (r, s) => new{r, s});
            var etco2_abpm = etco2.Join(abpm, (e, a) => new {e, a});

            var s_obs = pulse_hr
                    .Join(resp_spo2, (l, r) => new {l.h, l.p, r.r, r.s})
                    .Join(etco2_abpm, (l, r) => new {l.h, l.p, l.r, l.s, r.e, r.a})
                ;
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }
        
        static FOperation<MaskedSignal> Trans(FOperation<Signal> source)
        {
            var window = 60000;
            var iperiod = 8;
            var operiod = 2;
            var gap_tol = 60000;
            return source
                .Multicast(s => s
                    .Normalize(iperiod, window)
                    .Resample(iperiod, operiod, operiod)
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? gap_tol : e - s)
                    .Chop(operiod)
                    .Join(s.Mask(operiod, gap_tol),
                        (Signal sig, bool b, out MaskedSignal output) =>
                        {
                            output.signal = sig;
                            output.mask = b;
                        }
                    )
                );
        }

        class MSPair<T, U>
        {
            public T l;
            public U r;

            public MSPair(T l, U r)
            {
                this.l = l;
                this.r = r;
            }
        }

        static double CAPFuse(Func<IStreamable<Empty, Signal>> pulse_data,
            Func<IStreamable<Empty, Signal>> hr_data,
            Func<IStreamable<Empty, Signal>> resp_data,
            Func<IStreamable<Empty, Signal>> spo2_data,
            Func<IStreamable<Empty, Signal>> etco2_data,
            Func<IStreamable<Empty, Signal>> abpm_data)
        {
            var pulse = pulse_data().FuseStart();
            var hr = hr_data().FuseStart();
            var resp = resp_data().FuseStart();
            var spo2 = spo2_data().FuseStart();
            var etco2 = etco2_data().FuseStart();
            var abpm = abpm_data().FuseStart();

            var sw = new Stopwatch();
            sw.Start();
            var fpulse = Trans(pulse.GetFOP());
            var fhr = Trans(hr.GetFOP());
            var fresp = Trans(resp.GetFOP());
            var fspo2 = Trans(spo2.GetFOP());
            var fetco2 = Trans(etco2.GetFOP());
            var fabpm = Trans(abpm.GetFOP());

            var pulse_hr = fpulse.Join(fhr,
                (MaskedSignal left, MaskedSignal right, out MSPair<MaskedSignal, MaskedSignal> output) =>
                {
                    output = new MSPair<MaskedSignal, MaskedSignal>(left, right);
                });
            var resp_spo2 = fresp.Join(fspo2,
                (MaskedSignal left, MaskedSignal right, out MSPair<MaskedSignal, MaskedSignal> output) =>
                {
                    output = new MSPair<MaskedSignal, MaskedSignal>(left, right);
                });
            var etco2_abpm = fetco2.Join(fabpm,
                (MaskedSignal left, MaskedSignal right, out MSPair<MaskedSignal, MaskedSignal> output) =>
                {
                    output = new MSPair<MaskedSignal, MaskedSignal>(left, right);
                });

            var s_obs = pulse_hr
                    .Join(resp_spo2,
                        (MSPair<MaskedSignal, MaskedSignal> left, MSPair<MaskedSignal, MaskedSignal> right,
                            out MSPair<MSPair<MaskedSignal, MaskedSignal>, MSPair<MaskedSignal, MaskedSignal>>
                                output) =>
                        {
                            output =
                                new MSPair<MSPair<MaskedSignal, MaskedSignal>, MSPair<MaskedSignal, MaskedSignal>>(left,
                                    right);
                        })
                    .Join(etco2_abpm,
                        (MSPair<MSPair<MaskedSignal, MaskedSignal>, MSPair<MaskedSignal, MaskedSignal>> left,
                            MSPair<MaskedSignal, MaskedSignal> right,
                            out MSPair<MSPair<MSPair<MaskedSignal, MaskedSignal>, MSPair<MaskedSignal, MaskedSignal>>,
                                MSPair<MaskedSignal, MaskedSignal>> output) =>
                        {
                            output =
                                new MSPair<MSPair<MSPair<MaskedSignal, MaskedSignal>, MSPair<MaskedSignal, MaskedSignal>
                                >, MSPair<MaskedSignal, MaskedSignal>>(left, right);
                        })
                    .FuseEnd()
                ;

            pulse.Connect();
            hr.Connect();
            resp.Connect();
            spo2.Connect();
            etco2.Connect();
            abpm.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static void Main(string[] args)
        {
            Config.DataBatchSize = 60000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(1);
            Config.ForceRowBasedExecution = true;

            int duration = Int32.Parse(args[0]);
            var testcase = args[1].ToLower();
            var engine = args[2].ToLower();
            double time = 0;

            const int start = 0;
            const int freq = 125;
            const int period = 1000 / freq;
            const long window = 60000;
            const long gap_tol = window;
            long count = (duration * freq);
            Config.DataGranularity = window;

            Func<IStreamable<Empty, Signal>> data = () =>
            {
                var files = File.ReadAllLines("/home/anandj/data/code/sickkids/abpm2.txt");
                return files
                        .ToObservable()
                        .SelectMany(e => new CSVFileParser(e, "MDC_PRESS_BLD_ART_ABP", "3-73_1", 0, 0).Parse())
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
                    time = NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "normalize_lifestream":
                    time = FuseTest(data, stream =>
                            stream
                                .Normalize(period, window))
                        ;
                    break;
                case "passfilter_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "passfilter_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "fillconst_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillconst_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillmean_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "fillmean_lifestream":
                    time = FuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "resample_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2)
                    );
                    break;
                case "resample_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2, period)
                    );
                    break;
                case "endtoend_trill":
                    count = duration * (500 + 125);
                    time = MultiNonFuseTest(abp_data, ecg_data);
                    break;
                case "endtoend_lifestream":
                    count = duration * (500 + 125);
                    time = MultiFuseTest(abp_data, ecg_data);
                    break;
                case "sujay_trill":
                    time = Linezero(data, 600000, 60000);
                    break;
                case "sujay_lifestream":
                    time = LinezeroFuse(data, 600000, 60000);
                    break;
                case "cap_trill":
                    time = CAP(data, data, data, data, data, data);
                    break;
                case "cap_lifestream":
                    time = CAPFuse(data, data, data, data, data, data);
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0} on {1}", testcase, engine);
                    return;
            }
            Console.WriteLine("Benchmark: {0}, Engine: {1}, Data: {2} million events, Time: {3:.###} sec",
                testcase, engine, count, time);
            Config.StreamScheduler.Stop();
        }
    }
}
