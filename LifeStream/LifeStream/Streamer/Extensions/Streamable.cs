using System;
using System.Linq.Expressions;
using System.Reactive;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Attach aggregate results back to events
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="sourceSelector">Selector for source stream</param>
        /// <param name="aggregate">Aggregate function</param>
        /// <param name="resultSelector">Result selector</param>
        /// <param name="window">Window size</param>
        /// <param name="period">Period</param>
        /// <param name="offset">Offset</param>
        /// <returns>Signal stream after attaching aggregate result</returns>
        public static IStreamable<TKey, TOutput> AttachAggregate<TKey, TPayload, TInput, TState, TResult, TOutput>(
            this IStreamable<TKey, TPayload> source,
            Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TInput>> sourceSelector,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState, TResult>> aggregate,
            Expression<Func<TPayload, TResult, TOutput>> resultSelector,
            long window,
            long period,
            long offset = 0
        )
        {
            return source
                    .Multicast(s => s
                        .ShiftEventLifetime(offset)
                        .Join(sourceSelector(s)
                                .HoppingWindowLifetime(window, period, offset)
                                .Aggregate(aggregate),
                            resultSelector)
                        .ShiftEventLifetime(-offset)
                    )
                ;
        }

        /// <summary>
        /// Performs the 'Chop' operator to chop (partition) gap intervals across beat boundaries with gap tolerance.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="offset">Stream offset</param>
        /// <param name="period">Beat period to chop</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <returns>Signal stream after gaps chopped</returns>
        public static IStreamable<TKey, TPayload> Chop<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long offset,
            long period,
            long gap_tol)
        {
            gap_tol = Math.Max(period, gap_tol);
            return source
                    .AlterEventDuration((s, e) => e - s + gap_tol)
                    .Multicast(t => t.ClipEventDuration(t))
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? period : e - s)
                    .Chop(offset, period)
                ;
        }

        /// <summary>
        /// Index continuous intervals of the stream.
        /// </summary>
        /// <param name="source">Input stream</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <returns>Events corresponding to continuous intervals</returns>
        public static IStreamable<TKey, Unit> Index<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long gap_tol
        )
        {
            return source
                    .AlterEventDuration((s, e) => e - s + gap_tol)
                    .Multicast(s => s.ClipEventDuration(s))
                    .Select(e => Unit.Default)
                    .Stitch()
                    .AlterEventDuration((s, e) => e - s - gap_tol)
                ;
        }

        /// <summary>
        /// ConsecutivePairs with gap_tolerance
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="resultSelector">Compose result tuple using matching input events</param>
        /// <param name="gap_tol">Gap tolerance</param>
        /// <param name="period">Period of input stream (default: 1)</param>
        /// <returns>Pattern result stream</returns>
        public static IStreamable<TKey, TResult> ConsecutivePairs<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> stream,
            Expression<Func<TPayload, TPayload, TResult>> resultSelector,
            long gap_tol,
            long period = 1)
        {
            var clippedStream = stream
                    .AlterEventDuration((s, e) => e - s + gap_tol)
                    .Multicast(xs => xs.ClipEventDuration(xs))
                    .AlterEventDuration((s, e) => (e - s > gap_tol) ? period : e - s)
                ;

            var result = clippedStream
                .Multicast(xs => xs
                    .ShiftEventLifetime(1)
                    .Join(xs.AlterEventDuration(1), resultSelector));

            return result;
        }
    }
}