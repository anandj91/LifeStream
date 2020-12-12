using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public class BufferedTranslation<T, R> : IAggregate<T, BufferedTranslation<T, R>.PeriodState, R>
    {
        private readonly long period;
        private readonly Action<T[], R[]> translator;

        public class PeriodState
        {
            private R[] owin;
            private T[] iwin;
            private readonly long buf_size;
            private long ocount;
            private long icount;
            private readonly Action<T[], R[]> translator;
            private long checkpoint;

            internal PeriodState(long buf_size, Action<T[], R[]> translator)
            {
                this.owin = new R[buf_size];
                this.iwin = new T[buf_size];
                this.buf_size = buf_size;
                this.ocount = 0;
                this.icount = 0;
                this.translator = translator;
                this.checkpoint = 0;
            }

            private void CheckAndRefresh()
            {
                if (icount % buf_size != 0 || ocount % buf_size != 0 || icount < (checkpoint + buf_size)) return;
                translator(iwin, owin);
                checkpoint = icount;
            }

            internal PeriodState Acc(T t)
            {
                iwin[icount++ % buf_size] = t;
                return this;
            }

            internal R GetResult()
            {
                CheckAndRefresh();
                return owin[ocount % buf_size];
            }

            public PeriodState Deacc(T i)
            {
                ocount++;
                return this;
            }

            public PeriodState Sub(PeriodState o)
            {
                owin[ocount % buf_size] = default;
                ocount++; // Known issue: increment by 1 only works with a sliding window of step size 1.
                return this;
            }
        }

        public BufferedTranslation(long period, Action<T[], R[]> translator)
        {
            this.period = period;
            this.translator = translator;
        }

        public Expression<Func<PeriodState>> InitialState() => () => new PeriodState(period, translator);

        public Expression<Func<PeriodState, long, T, PeriodState>> Accumulate() => (s, t, i) => s.Acc(i);

        public Expression<Func<PeriodState, long, T, PeriodState>> Deaccumulate() => (s, t, i) => s.Deacc(i);

        public Expression<Func<PeriodState, PeriodState, PeriodState>> Difference() => (s1, s2) => s1.Sub(s2);

        public Expression<Func<PeriodState, R>> ComputeResult() => s => s.GetResult();
    }
}