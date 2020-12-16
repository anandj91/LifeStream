// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// Abstract class used by aggregates that compute results on a list.
    /// </summary>
    /// <typeparam name="T">Input type</typeparam>
    /// <typeparam name="R">Result type</typeparam>
    public abstract class ListAggregateBase<T, R> : IAggregate<T, List<T>, R>
    {
        private static readonly Expression<Func<List<T>>> init = () => new List<T>();
        /// <summary>
        /// Provides an expression that creates the initial state for the aggregate computation.
        /// </summary>
        /// <returns>An expression that creates the initial state for the aggregate computation.</returns>
        public Expression<Func<List<T>>> InitialState() => init;

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a new data object and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a new data object and compute a new aggregate state.</returns>
        public Expression<Func<List<T>, long, T, List<T>>> Accumulate()
        {
            Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Add(input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
        }

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a retracted data object and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a retracted data object and compute a new aggregate state.</returns>
        public Expression<Func<List<T>, long, T, List<T>>> Deaccumulate()
        {
            Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Remove(input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
        }

        /// <summary>
        /// Provides an expression that describes how to take two different aggregate states and subtract one from the other.
        /// </summary>
        /// <returns>An expression that describes how to take two different aggregate states and subtract one from the other.</returns>
        public Expression<Func<List<T>, List<T>, List<T>>> Difference() => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

        private static List<T> SetExcept(List<T> left, List<T> right)
        {
            var newList = new List<T>(left);
            foreach (var t in right) newList.Remove(t);
            return newList;
        }

        /// <summary>
        /// Provides an expression that describes how to compute a final result from an aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to compute a final result from an aggregate state.</returns>
        public abstract Expression<Func<List<T>, R>> ComputeResult();
    }
}
