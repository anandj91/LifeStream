using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public class BatchState<T>
    {
        public List<long> times;
        public List<T> values;

        public BatchState()
        {
            this.values = new List<T>();
            this.times = new List<long>();
        }

        public BatchState<T> Add(long t, T v)
        {
            values.Add(v);
            times.Add(t);
            return this;
        }

        public BatchState<T> Sub(long t, T v)
        {
            values.RemoveRange(0, 1);
            times.RemoveRange(0, 1);
            return this;
        }

        public BatchState<T> Diff(BatchState<T> s2)
        {
            var idx = 0;
            if (s2.times.Count > 0)
            {
                idx = times.IndexOf(s2.times[^1]) + 1;
            }

            times.RemoveRange(0, idx);
            values.RemoveRange(0, idx);

            return this;
        }
    }

    public class BatchIAggregate<T> : IAggregate<T, BatchState<T>, T[]>
    {
        public Expression<Func<BatchState<T>>> InitialState() => () => new BatchState<T>();

        public Expression<Func<BatchState<T>, long, T, BatchState<T>>> Accumulate() => (s, t, v) => s.Add(t, v);

        public Expression<Func<BatchState<T>, long, T, BatchState<T>>> Deaccumulate() => (s, t, v) => s.Sub(t, v);

        public Expression<Func<BatchState<T>, BatchState<T>, BatchState<T>>> Difference() => (s1, s2) => s1.Diff(s2);

        public Expression<Func<BatchState<T>, T[]>> ComputeResult() => s => s.values.ToArray();
    }
}