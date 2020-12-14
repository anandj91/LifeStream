﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information. 

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace System.Linq
{
    public static partial class AsyncEnumerable
    {
        /// <summary>
        /// Correlates the elements of two sequences based on matching keys. The default equality comparer is used to compare keys.
        /// </summary>
        /// <typeparam name="TOuter">The type of the elements of the first async-enumerable sequence.</typeparam>
        /// <typeparam name="TInner">The type of the elements of the second async-enumerable sequence.</typeparam>
        /// <typeparam name="TKey">The type of the keys returned by the key selector functions.</typeparam>
        /// <typeparam name="TResult">The type of the result elements.</typeparam>
        /// <param name="outer">The first async-enumerable sequence to join.</param>
        /// <param name="inner">The async-enumerable sequence to join to the first sequence.</param>
        /// <param name="outerKeySelector">A function to extract the join key from each element of the first sequence.</param>
        /// <param name="innerKeySelector">A function to extract the join key from each element of the second sequence.</param>
        /// <param name="resultSelector">A function to create a result element from two matching elements.</param>
        /// <returns>An async-enumerable sequence that has elements of type TResult that are obtained by performing an inner join on two sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="outer"/> or <paramref name="inner"/> or <paramref name="outerKeySelector"/> or <paramref name="innerKeySelector"/> or <paramref name="resultSelector"/> is null.</exception>
        public static IAsyncEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector) =>
            Join(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer: null);

        /// <summary>
        /// Correlates the elements of two sequences based on matching keys. A specified equality comparer is used to compare keys.
        /// </summary>
        /// <typeparam name="TOuter">The type of the elements of the first async-enumerable sequence.</typeparam>
        /// <typeparam name="TInner">The type of the elements of the second async-enumerable sequence.</typeparam>
        /// <typeparam name="TKey">The type of the keys returned by the key selector functions.</typeparam>
        /// <typeparam name="TResult">The type of the result elements.</typeparam>
        /// <param name="outer">The first async-enumerable sequence to join.</param>
        /// <param name="inner">The async-enumerable sequence to join to the first sequence.</param>
        /// <param name="outerKeySelector">A function to extract the join key from each element of the first sequence.</param>
        /// <param name="innerKeySelector">A function to extract the join key from each element of the second sequence.</param>
        /// <param name="resultSelector">A function to create a result element from two matching elements.</param>
        /// <param name="comparer">An equality comparer to hash and compare keys.</param>
        /// <returns>An async-enumerable sequence that has elements of type TResult that are obtained by performing an inner join on two sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="outer"/> or <paramref name="inner"/> or <paramref name="outerKeySelector"/> or <paramref name="innerKeySelector"/> or <paramref name="resultSelector"/> is null.</exception>
        public static IAsyncEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector, IEqualityComparer<TKey>? comparer)
        {
            if (outer == null)
                throw Error.ArgumentNull(nameof(outer));
            if (inner == null)
                throw Error.ArgumentNull(nameof(inner));
            if (outerKeySelector == null)
                throw Error.ArgumentNull(nameof(outerKeySelector));
            if (innerKeySelector == null)
                throw Error.ArgumentNull(nameof(innerKeySelector));
            if (resultSelector == null)
                throw Error.ArgumentNull(nameof(resultSelector));

            return Create(Core);

            async IAsyncEnumerator<TResult> Core(CancellationToken cancellationToken)
            {
                await using var e = outer.GetConfiguredAsyncEnumerator(cancellationToken, false);

                if (await e.MoveNextAsync())
                {
                    var lookup = await Internal.Lookup<TKey, TInner>.CreateForJoinAsync(inner, innerKeySelector, comparer, cancellationToken).ConfigureAwait(false);

                    if (lookup.Count != 0)
                    {
                        do
                        {
                            var item = e.Current;

                            var outerKey = outerKeySelector(item);

                            var g = lookup.GetGrouping(outerKey);

                            if (g != null)
                            {
                                var count = g._count;
                                var elements = g._elements;

                                for (var i = 0; i != count; ++i)
                                {
                                    yield return resultSelector(item, elements[i]);
                                }
                            }
                        }
                        while (await e.MoveNextAsync());
                    }
                }
            }
        }

        internal static IAsyncEnumerable<TResult> JoinAwaitCore<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, ValueTask<TKey>> outerKeySelector, Func<TInner, ValueTask<TKey>> innerKeySelector, Func<TOuter, TInner, ValueTask<TResult>> resultSelector) =>
            JoinAwaitCore<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer: null);

        internal static IAsyncEnumerable<TResult> JoinAwaitCore<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, ValueTask<TKey>> outerKeySelector, Func<TInner, ValueTask<TKey>> innerKeySelector, Func<TOuter, TInner, ValueTask<TResult>> resultSelector, IEqualityComparer<TKey>? comparer)
        {
            if (outer == null)
                throw Error.ArgumentNull(nameof(outer));
            if (inner == null)
                throw Error.ArgumentNull(nameof(inner));
            if (outerKeySelector == null)
                throw Error.ArgumentNull(nameof(outerKeySelector));
            if (innerKeySelector == null)
                throw Error.ArgumentNull(nameof(innerKeySelector));
            if (resultSelector == null)
                throw Error.ArgumentNull(nameof(resultSelector));

            return Create(Core);

            async IAsyncEnumerator<TResult> Core(CancellationToken cancellationToken)
            {
                await using var e = outer.GetConfiguredAsyncEnumerator(cancellationToken, false);

                if (await e.MoveNextAsync())
                {
                    var lookup = await Internal.LookupWithTask<TKey, TInner>.CreateForJoinAsync(inner, innerKeySelector, comparer, cancellationToken).ConfigureAwait(false);

                    if (lookup.Count != 0)
                    {
                        do
                        {
                            var item = e.Current;

                            var outerKey = await outerKeySelector(item).ConfigureAwait(false);

                            var g = lookup.GetGrouping(outerKey);

                            if (g != null)
                            {
                                var count = g._count;
                                var elements = g._elements;

                                for (var i = 0; i != count; ++i)
                                {
                                    yield return await resultSelector(item, elements[i]).ConfigureAwait(false);
                                }
                            }
                        }
                        while (await e.MoveNextAsync());
                    }
                }
            }
        }

#if !NO_DEEP_CANCELLATION
        internal static IAsyncEnumerable<TResult> JoinAwaitWithCancellationCore<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, CancellationToken, ValueTask<TKey>> outerKeySelector, Func<TInner, CancellationToken, ValueTask<TKey>> innerKeySelector, Func<TOuter, TInner, CancellationToken, ValueTask<TResult>> resultSelector) =>
            JoinAwaitWithCancellationCore<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer: null);

        internal static IAsyncEnumerable<TResult> JoinAwaitWithCancellationCore<TOuter, TInner, TKey, TResult>(this IAsyncEnumerable<TOuter> outer, IAsyncEnumerable<TInner> inner, Func<TOuter, CancellationToken, ValueTask<TKey>> outerKeySelector, Func<TInner, CancellationToken, ValueTask<TKey>> innerKeySelector, Func<TOuter, TInner, CancellationToken, ValueTask<TResult>> resultSelector, IEqualityComparer<TKey>? comparer)
        {
            if (outer == null)
                throw Error.ArgumentNull(nameof(outer));
            if (inner == null)
                throw Error.ArgumentNull(nameof(inner));
            if (outerKeySelector == null)
                throw Error.ArgumentNull(nameof(outerKeySelector));
            if (innerKeySelector == null)
                throw Error.ArgumentNull(nameof(innerKeySelector));
            if (resultSelector == null)
                throw Error.ArgumentNull(nameof(resultSelector));

            return Create(Core);

            async IAsyncEnumerator<TResult> Core(CancellationToken cancellationToken)
            {
                await using var e = outer.GetConfiguredAsyncEnumerator(cancellationToken, false);

                if (await e.MoveNextAsync())
                {
                    var lookup = await Internal.LookupWithTask<TKey, TInner>.CreateForJoinAsync(inner, innerKeySelector, comparer, cancellationToken).ConfigureAwait(false);

                    if (lookup.Count != 0)
                    {
                        do
                        {
                            var item = e.Current;

                            var outerKey = await outerKeySelector(item, cancellationToken).ConfigureAwait(false);

                            var g = lookup.GetGrouping(outerKey);

                            if (g != null)
                            {
                                var count = g._count;
                                var elements = g._elements;

                                for (var i = 0; i != count; ++i)
                                {
                                    yield return await resultSelector(item, elements[i], cancellationToken).ConfigureAwait(false);
                                }
                            }
                        }
                        while (await e.MoveNextAsync());
                    }
                }
            }
        }
#endif
    }
}
