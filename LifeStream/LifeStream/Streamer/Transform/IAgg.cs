using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public interface IAggState<T, R>
    {
        public IAggState<T, R> Reset();

        public IAggState<T, R> Acc(IAggState<T, R> s, long t, T i);

        public R Res();
    }

    public class IAgg<T, R> : IAggregate<T, IAggState<T, R>, R>
    {
        protected IAggState<T, R> state;

        public IAgg(IAggState<T, R> state)
        {
            this.state = state;
        }

        public Expression<Func<IAggState<T, R>>> InitialState() => () => state.Reset();

        public Expression<Func<IAggState<T, R>, long, T, IAggState<T, R>>> Accumulate()
            => (s, t, i) => state.Acc(s, t, i);

        public Expression<Func<IAggState<T, R>, long, T, IAggState<T, R>>> Deaccumulate()
        {
            throw new NotImplementedException();
        }

        public Expression<Func<IAggState<T, R>, IAggState<T, R>, IAggState<T, R>>> Difference()
        {
            throw new NotImplementedException();
        }

        public Expression<Func<IAggState<T, R>, R>> ComputeResult()
            => s => state.Res();
    }
}