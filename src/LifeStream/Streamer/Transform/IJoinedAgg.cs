using System;
using Streamer.Ingest;

namespace Microsoft.StreamProcessing
{
    public class JoinedAggState<R1, R2, R> : IAggState<Signal, R>
    {
        private IAggState<Signal, R1> state1;
        private IAggState<Signal, R2> state2;
        private Func<R1, R2, R> resultSelector;

        public JoinedAggState(
            IAggState<Signal, R1> state1, IAggState<Signal, R2> state2, Func<R1, R2, R> resultSelector
        )
        {
            this.state1 = state1;
            this.state2 = state2;
            this.resultSelector = resultSelector;
        }

        public IAggState<Signal, R> Reset()
        {
            state1.Reset();
            state2.Reset();
            return this;
        }

        public IAggState<Signal, R> Acc(IAggState<Signal, R> s, long t, Signal i)
        {
            state1.Acc(state1, t, i);
            state2.Acc(state2, t, i);
            return this;
        }

        public R Res() => resultSelector(state1.Res(), state2.Res());
    }

    public class IJoinedAgg<R1, R2, R> : IAgg<Signal, R>
    {
        public IJoinedAgg(IAgg<Signal, R1> agg1, IAgg<Signal, R2> agg2, Func<R1, R2, R> resultSelector)
            : base(new JoinedAggState<R1, R2, R>(
                    agg1.InitialState().Compile()(),
                    agg2.InitialState().Compile()(),
                    resultSelector
                )
            )
        {
        }
    }
}