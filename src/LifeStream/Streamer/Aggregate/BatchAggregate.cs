using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public class BatchAggregate<T> : ListAggregateBase<T, List<T>>
    {
        public override Expression<Func<List<T>, List<T>>> ComputeResult() => state => state;
    }
}