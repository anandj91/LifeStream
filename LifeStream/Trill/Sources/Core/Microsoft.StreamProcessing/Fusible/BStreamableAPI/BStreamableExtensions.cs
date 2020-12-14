using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public static class BStreamableExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static BStreamable<TResult> Select<TPayload, TResult>(
            this BStreamable<TPayload> source,
            Expression<Func<TPayload, TResult>> selector
        )
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return new SelectBStream<TPayload, TResult>(source, selector.Compile());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="predicate"></param>
        /// <typeparam name="TPayload"></typeparam>
        /// <returns></returns>
        public static BStreamable<TPayload> Where<TPayload>(
            this BStreamable<TPayload> source, Expression<Func<TPayload, bool>> predicate)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(predicate, nameof(predicate));

            return new WhereBStream<TPayload>(source, predicate.Compile());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static BStreamable<TResult> Multicast<TPayload, TResult>(
            this BStreamable<TPayload> source,
            Func<BStreamable<TPayload>, BStreamable<TResult>> selector
        )
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return selector(source);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="window"></param>
        /// <param name="offset"></param>
        /// <typeparam name="TPayload"></typeparam>
        /// <returns></returns>
        public static TumblingWindowBStream<TPayload> TumblingWindowLifetime<TPayload>(
            this BStreamable<TPayload> source,
            long window,
            long offset = 0
        )
        {
            Invariant.IsNotNull(source, nameof(source));

            return new TumblingWindowBStream<TPayload>(source, window, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="aggregate"></param>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TAggState"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static BStreamable<TResult> Aggregate<TPayload, TAggState, TResult>(
            this TumblingWindowBStream<TPayload> source,
            Func<Window<Empty, TPayload>, IAggregate<TPayload, TAggState, TResult>> aggregate
        )
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate, nameof(aggregate));
            var p = new StreamProperties<Empty, TPayload>(
                false, true, source.Period, true, source.Period, source.Offset,
                false, true, true, true,
                EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<TPayload>.Default,
                ComparerExpression<Empty>.Default, ComparerExpression<TPayload>.Default, null, null, null
            );
            return new AggregateBStream<TPayload, TAggState, TResult>(
                source, aggregate(new Window<Empty, TPayload>(p))
            );
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="resultSelector"></param>
        /// <typeparam name="TLeft"></typeparam>
        /// <typeparam name="TRight"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static BStreamable<TResult> Join<TLeft, TRight, TResult>(
            this BStreamable<TLeft> left,
            BStreamable<TRight> right,
            Func<TLeft, TRight, TResult> resultSelector
        )
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return new JoinBStream<TLeft, TRight, TResult>(left, right, resultSelector);
        }
    }
}