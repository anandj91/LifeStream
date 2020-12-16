using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// 
    /// </summary>
    public class StdScore
    {
        /// <summary>
        /// 
        /// </summary>
        public double avg
        {
            get { return sum / count; }
        }

        /// <summary>
        /// 
        /// </summary>
        public double std
        {
            get { return Math.Sqrt((sumsq - (sum * sum / count)) / count); }
        }

        private long count;
        private double sum;
        private double sumsq;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <param name="sum"></param>
        /// <param name="sumsq"></param>
        public StdScore(long count, double sum, double sumsq)
        {
            this.count = count;
            this.sum = sum;
            this.sumsq = sumsq;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        public StdScore Acc(double x)
        {
            return new StdScore(count + 1, sum + x, sumsq + (x * x));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        public StdScore Deacc(double x)
        {
            return new StdScore(count - 1, sum - x, sumsq - (x * x));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public StdScore Diff(StdScore s)
        {
            return new StdScore(count - s.count, sum - s.sum, sumsq - s.sumsq);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return $"{nameof(avg)}: {avg}, {nameof(std)}: {std}";
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class StdScoreAggregate : IAggregate<float, StdScore, StdScore>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<StdScore>> InitialState() => () => new StdScore(0, 0, 0);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<StdScore, long, float, StdScore>> Accumulate() => (s, t, x) => s.Acc(x);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<StdScore, long, float, StdScore>> Deaccumulate() => (s, t, x) => s.Deacc(x);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<StdScore, StdScore, StdScore>> Difference() => (s1, s2) => s1.Diff(s2);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Expression<Func<StdScore, StdScore>> ComputeResult() => s => s;
    }
}