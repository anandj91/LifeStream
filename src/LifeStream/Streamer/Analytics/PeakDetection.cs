using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using Microsoft.StreamProcessing;
using Streamer.Ingest;

namespace Streamer.Analytics
{
    public static class PeakDetection
    {
        /// <summary>
        /// Peak detection query.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="period">Period of input stream</param>
        /// <param name="window">Mean window</param>
        /// <param name="factor">Scaling factor</param>
        /// <returns>Peaks in the signal stream</returns>
        public static IStreamable<Empty, Signal> Peaks(
            this IStreamable<Empty, Signal> source,
            long period,
            float factor,
            long window = 400
        )
        {
            return source
                    .AttachAggregate(
                        s => s.Select(e => e.val),
                        w => w.Average(e => e * factor),
                        (signal, avg) => new {signal, avg},
                        2 * window, 1, window
                    )
                    .Where(e => e.signal.val > e.avg)
                    .Select(e => e.signal)
                    .StitchAggregate(w => w.TopK(e => e.val, 1))
                    .Select(e => e.First().Payload)
                    .AlterEventDuration(period)
                ;
        }

        internal class RR
        {
            public Signal s_signal;
            public Signal e_signal;
            public double rr;

            public RR(Signal s_signal, Signal e_signal, double rr)
            {
                this.s_signal = s_signal;
                this.e_signal = e_signal;
                this.rr = rr;
            }
        }

        internal class BatchedRR
        {
            public List<RR> rrs;
            public double? std;
            public double avg;

            public BatchedRR(List<RR> rrs, double? std, double avg)
            {
                this.rrs = rrs;
                this.std = std;
                this.avg = avg;
            }
        }

        internal static IEnumerable<Signal> FilterRR(List<RR> rrs, double avg)
        {
            var lb = avg - Math.Max(avg * 0.3, 300);
            var ub = avg + Math.Max(avg * 0.3, 300);
            return rrs
                    .Where(v => v.rr > lb && v.rr < ub)
                    .Select(e => e.s_signal)
                    .Append(rrs[^1].e_signal)
                ;
        }

        /// <summary>
        /// Advanced peak detection query.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="mean_window">Mean window</param>
        /// <param name="rr_window">Window on RR interval list</param>
        /// <param name="period">Period</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="bpmmin">Lower limit of BPM</param>
        /// <param name="bpmmax">Upper limit of BPM</param>
        /// <returns>Peaks in the signal stream</returns>
        public static IStreamable<Empty, Signal> Peaks(
            this IStreamable<Empty, Signal> source,
            long period,
            long gap_tol,
            long rr_window,
            long mean_window = 400,
            float bpmmin = 40,
            float bpmmax = 180
        )
        {
            var factors = new List<float>
            {
                1.05f, 1.10f, 1.15f, 1.20f, 1.25f, 1.30f, 1.40f, 1.50f, 1.60f,
                1.70f, 1.80f, 1.90f, 2.00f, 2.10f, 2.20f, 2.50f, 3.00f, 4.00f
            };
            var sources = source.Multicast(factors.Count);
            var peaks = new List<IStreamable<Empty, BatchedRR>>();

            for (int i = 0; i < factors.Count; i++)
            {
                var peak = sources[i]
                        .Peaks(period, factors[i], mean_window)
                        .ConsecutivePairs((l, r) => new RR(l, r, r.ts - l.ts), gap_tol)
                        .AlterEventDuration(period)
                        .HoppingWindowLifetime(rr_window, rr_window)
                        .Aggregate(w => w
                            .JoinedAggregate(
                                new BatchAggregate<RR>(),
                                w.JoinedAggregate(
                                    w.Average(e => e.rr),
                                    w.JoinedAggregate(
                                        w.StandardDeviation(e => e.rr),
                                        w.Count(),
                                        (std, cnt) => new {std, cnt}),
                                    (avg, r) => new {avg, r.std, r.cnt}
                                ),
                                (batch, r) => new {batch, r.avg, r.std, bpm = (double) r.cnt * 60000 / rr_window}
                            )
                        )
                        .Where(e => e.std > 0.1 && e.bpm >= bpmmin && e.bpm <= bpmmax)
                        .Select(e => new BatchedRR(e.batch, e.std, e.avg))
                        .ShiftEventLifetime(-rr_window)
                    ;
                peaks.Add(peak);
            }

            var sel_peaks = peaks[0];
            for (int i = 1; i < peaks.Count; i++)
            {
                sel_peaks = sel_peaks
                        .LeftOuterJoin(peaks[i], e => Unit.Default, e => Unit.Default,
                            l => l, (l, r) => (l.std < r.std) ? l : r)
                    ;
            }

            var cor_peaks = source
                    .Multicast(s => s
                        .Join(sel_peaks
                                .SelectMany(e => FilterRR(e.rrs, e.avg)),
                            e => e.ts, e => e.ts,
                            (l, r) => l
                        )
                    )
                ;

            return cor_peaks;
        }
    }
}