using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An aggregate that combines results of other two aggregates
    /// </summary>
    /// <typeparam name="T">Input type</typeparam>
    /// <typeparam name="S1">State type of first aggregate</typeparam>
    /// <typeparam name="S2">State type of second aggregate</typeparam>
    /// <typeparam name="R1">Result type of first aggregate</typeparam>
    /// <typeparam name="R2">Result type of second aggregate</typeparam>
    /// <typeparam name="R">Joined result type</typeparam>
    public class JoinedAggregate<T, S1, S2, R1, R2, R> : IAggregate<T, (S1, S2), R>
    {
        private Func<S1> init1;
        private Func<S2> init2;
        private Func<S1, long, T, S1> acc1;
        private Func<S2, long, T, S2> acc2;
        private Func<S1, long, T, S1> deacc1;
        private Func<S2, long, T, S2> deacc2;
        private Func<S1, S1, S1> diff1;
        private Func<S2, S2, S2> diff2;
        private Func<S1, R1> compute1;
        private Func<S2, R2> compute2;

        private Func<R1, R2, R> resultSelector;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="agg1">First aggregate function</param>
        /// <param name="agg2">Second aggregate function</param>
        /// <param name="resultSelector">Result selector</param>
        public JoinedAggregate(
            IAggregate<T, S1, R1> agg1,
            IAggregate<T, S2, R2> agg2,
            Func<R1, R2, R> resultSelector
            )
        {
            this.init1 = agg1.InitialState().Compile();
            this.init2 = agg2.InitialState().Compile();
            this.acc1 = agg1.Accumulate().Compile();
            this.acc2 = agg2.Accumulate().Compile();
            this.deacc1 = agg1.Deaccumulate().Compile();
            this.deacc2 = agg2.Deaccumulate().Compile();
            this.diff1 = agg1.Difference().Compile();
            this.diff2 = agg2.Difference().Compile();
            this.compute1 = agg1.ComputeResult().Compile();
            this.compute2 = agg2.ComputeResult().Compile();
            this.resultSelector = resultSelector;
        }

        private (S1, S2) Init()
        {
            return (init1(), init2());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<(S1, S2)>> InitialState() => () => Init();

        private (S1, S2) Acc((S1, S2) states, long t, T input)
        {
            return (acc1(states.Item1, t, input), acc2(states.Item2, t, input));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<(S1, S2), long, T, (S1, S2)>> Accumulate() => (s, t, i) => Acc(s, t, i);

        private (S1, S2) Deacc((S1, S2) states, long t, T input)
        {
            return (deacc1(states.Item1, t, input), deacc2(states.Item2, t, input));
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<(S1, S2), long, T, (S1, S2)>> Deaccumulate() => (s, t, i) => Deacc(s, t, i);

        private (S1, S2) Diff((S1, S2) states1, (S1, S2) states2)
        {
            return (diff1(states1.Item1, states2.Item1), diff2(states1.Item2, states2.Item2));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<(S1, S2), (S1, S2), (S1, S2)>> Difference() => (s1, s2) => Diff(s1, s2);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<(S1, S2), R>> ComputeResult() =>
            (s) => resultSelector(compute1(s.Item1), compute2(s.Item2));
    }
}