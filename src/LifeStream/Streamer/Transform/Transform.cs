using System;
using System.Collections.Generic;
using MathNet.Filtering;
using Streamer.Ingest;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Resample signal from one frequency to a different one.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="iperiod">Period of input signal stream</param>
        /// <param name="operiod">Period of output signal stream</param>
        /// <param name="offset">Offset</param>
        /// <returns>Result (output) stream in the new signal frequency</returns>
        public static IStreamable<Empty, Signal> Resample(
            this IStreamable<Empty, Signal> source,
            long iperiod,
            long operiod,
            long offset = 0)
        {
            return source
                    .Multicast(s => s.ClipEventDuration(s))
                    .Multicast(s => s
                        .ShiftEventLifetime(1)
                        .Join(s
                                .AlterEventDuration(1),
                            (l, r) => new {st = l.ts, sv = l.val, et = r.ts, ev = r.val}))
                    .AlterEventLifetime(t => t - iperiod, iperiod)
                    .Chop(offset, operiod)
                    .HoppingWindowLifetime(1, operiod)
                    .AlterEventDuration(operiod)
                    .Select((t, e) => new Signal(t, ((e.ev - e.sv) * (t - e.st) / (e.et - e.st) + e.sv)))
                ;
        }

        /// <summary>
        /// Normalize a signal using standard score.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="window">Normalization window</param>
        /// <returns>Normalized signal</returns>
        public static IStreamable<Empty, Signal> Normalize(
            this IStreamable<Empty, Signal> source,
            long window
        )
        {
            return source
                    .AttachAggregate(
                        s => s.Select(e => e.val),
                        w => w.JoinedAggregate(
                            w.Average(e => e),
                            w.StandardDeviation(e => e),
                            (avg, std) => new {avg, std}
                        ),
                        (signal, agg) =>
                            new Signal(signal.ts, (float) ((signal.val - agg.avg) / agg.std)),
                        window, window, window - 1
                    )
                ;
        }

        /// <summary>
        /// Fill missing values with a constant.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="val">Filler value</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<Empty, Signal> FillConst(
            this IStreamable<Empty, Signal> source,
            long period,
            long gap_tol,
            float val,
            long offset = 0)
        {
            return source
                    .Chop(offset, period, gap_tol)
                    .Select((t, s) => (t == s.ts) ? s : new Signal(t, val))
                ;
        }

        /// <summary>
        /// Fill missing values with mean of historic values.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="window">Mean window</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<Empty, Signal> FillMean(
            this IStreamable<Empty, Signal> source,
            long window,
            long period,
            long gap_tol,
            long offset = 0
        )
        {
            return source
                    .AttachAggregate(s => s, w => w.Average(e => e.val),
                        (signal, avg) => new {signal, avg, sqd = (signal.val - avg) * (signal.val - avg)},
                        window, window, window-1)
                    .AlterEventDuration(period)
                    .Chop(offset, period, gap_tol)
                    .Select((vs, s) => new {s.signal, s.avg, new_ts = vs})
                    .Select(e =>
                        (e.signal.ts == e.new_ts)
                            ? e.signal
                            : new Signal(e.new_ts, e.avg))
                    .AlterEventDuration(period)
                ;
        }

        /// <summary>
        /// Calculate masking bits for missing values.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input signal stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal after missing values filled with `val`</returns>
        public static IStreamable<Empty, bool> Mask(
            this IStreamable<Empty, Signal> source,
            long period,
            long gap_tol,
            long offset = 0)
        {
            return source
                    .Chop(offset, period, gap_tol)
                    .Select((t, s) => t != s.ts)
                ;
        }

        private static List<Signal> FreqFilter(List<Signal> input, OnlineFilter bp)
        {
            var len = input.Count;
            var ival = new double[len];
            var output = new List<Signal>(len);
            for (int i = 0; i < len; i++)
            {
                ival[i] = input[i].val;
            }

            var new_val = bp.ProcessSamples(ival);
            for (int k = 0; k < new_val.Length; k++)
            {
                output.Add(new Signal(input[k].ts, (float) new_val[k]));
            }

            return output;
        }

        /// <summary>
        /// Frequency filter.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input stream</param>
        /// <param name="window">Window size</param>
        /// <param name="filter">Filter function to select in frequencies</param>
        /// <returns>Signal after frequency filter pass.</returns>
        public static IStreamable<Empty, Signal> BandPassFilter(
            this IStreamable<Empty, Signal> source,
            long period,
            long window,
            double low,
            double high
        )
        {
            var bp = OnlineFilter.CreateBandpass(ImpulseResponse.Finite, period, low, high);
            return source
                    .Multicast(s => s
                        .ShiftEventLifetime(window)
                        .Join(s
                                .TumblingWindowLifetime(window, window)
                                .Aggregate(w => new BatchAggregate<Signal>())
                                .Select(input => FreqFilter(input, bp))
                                .SelectMany(e => e),
                            l => l.ts, r => r.ts,
                            (l, r) => r
                        )
                    )
                    .ShiftEventLifetime(-window)
                ;
        }
    }
}