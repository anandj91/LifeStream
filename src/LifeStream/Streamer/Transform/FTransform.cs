using System;
using MathNet.Filtering;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;

namespace Microsoft.StreamProcessing
{
    public static class FStreamable
    {
        public struct SigPair
        {
            public Signal s;
            public Signal e;

            public override string ToString()
            {
                return $"{nameof(s)}: {s}, {nameof(e)}: {e}";
            }
        }

        internal static void ResampleJoiner(Signal l, Signal r, out SigPair o)
        {
            o.s = l;
            o.e = r;
        }

        internal static void ResampleSelector(long t, SigPair p, out Signal o)
        {
            o.ts = t;
            o.val = ((p.e.val - p.s.val) * (t - p.s.ts) / (p.e.ts - p.s.ts) + p.s.val);
        }

        public static FOperation<Signal> Resample(
            this FOperation<Signal> source,
            long iperiod,
            long operiod,
            long gap_tol
        )
        {
            return source
                    .ConsecutivePairs<Signal, SigPair>(ResampleJoiner)
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? iperiod : e - s)
                    .Chop(1)
                    .Select<SigPair, Signal>(ResampleSelector)
                    .AlterPeriod(operiod)
                ;
        }

        internal static void NormalizeJoiner(Signal signal, (float avg, float std) agg, out Signal o)
        {
            o.ts = signal.ts;
            o.val = ((signal.val - agg.avg) / agg.std);
        }

        public static FOperation<Signal> Normalize(
            this FOperation<Signal> source,
            long period,
            long window
        )
        {
            return source
                    .Multicast(s => s
                        .Join<Signal, (float avg, float std), Signal>(s
                                .Aggregate(w =>
                                        new IJoinedAgg<float, float, (float avg, float std)>(
                                            new IAverage(), new IStdDev(period, window), (avg, std) => (avg, std)
                                        ), window, window
                                ),
                            NormalizeJoiner
                        )
                    )
                ;
        }

        internal static void FillJoiner<T>(T l, T r, out T o) => o = l;

        public static FOperation<Signal> FillConst(
            this FOperation<Signal> source,
            long period,
            long gap_tol,
            float val
        )
        {
            void FillConstSelector(long t, Signal s, out Signal o)
            {
                if (t == s.ts)
                {
                    o = s;
                }
                else
                {
                    o.ts = t;
                    o.val = val;
                }
            }

            return source
                    .ConsecutivePairs<Signal, Signal>(FillJoiner)
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? period : e - s)
                    .Chop(period)
                    .Select<Signal, Signal>(FillConstSelector)
                ;
        }

        public struct SignalAvg
        {
            public Signal signal;
            public float avg;

            public override string ToString()
            {
                return $"{nameof(signal)}: {signal}, {nameof(avg)}: {avg}";
            }
        }

        internal static void FillMeanJoiner(Signal signal, float avg, out SignalAvg o)
        {
            o.signal = signal;
            o.avg = avg;
        }

        internal static void FillMeanSelector(long t, SignalAvg s, out Signal o)
        {
            if (t == s.signal.ts)
            {
                o = s.signal;
            }
            else
            {
                o.ts = t;
                o.val = s.avg;
            }
        }

        public static FOperation<Signal> FillMean(
            this FOperation<Signal> source,
            long window,
            long period,
            long gap_tol
        )
        {
            return source
                    .Multicast(s => s
                        .Join<Signal, float, SignalAvg>(s
                                .Aggregate(w => new IAverage(), window, window),
                            FillMeanJoiner
                        )
                    )
                    .ConsecutivePairs<SignalAvg, SignalAvg>(FillJoiner)
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? period : e - s)
                    .Chop(period)
                    .Select<SignalAvg, Signal>(FillMeanSelector)
                ;
        }

        public static FOperation<Signal> BandPassFilter(
            this FOperation<Signal> source,
            long period,
            long window,
            double low,
            double high
        )
        {
            var bp = OnlineFilter.CreateBandpass(ImpulseResponse.Finite, period, low, high);
            return source
                    .Transform<Signal, Signal>(window,
                        (ioff, input, off, output, len) =>
                        {
                            var ival = new double[len];
                            for (int i = 0; i < len; i++)
                            {
                                ival[i] = input[ioff + i].val;
                            }

                            var new_val = bp.ProcessSamples(ival);
                            for (int k = 0; k < new_val.Length; k++)
                            {
                                output[off + k].ts = input[ioff + k].ts;
                                output[off + k].val = (float) new_val[k];
                            }
                        }
                    )
                ;
        }
    }
}