using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    internal sealed class FEndStreamable<TResult> : Streamable<Empty, TResult>
    {
        private IStreamObserver<Empty, TResult> Observer;
        internal OutputFWindow<TResult> owindow;
        private readonly MemoryPool<Empty, TResult> pool;
        [DataMember] private StreamMessage<Empty, TResult> output;

        public FEndStreamable(FOperation<TResult> fop)
            : base(new StreamProperties<Empty, TResult>(
                false,
                false, null,
                false, null, null,
                false, false,
                false, false,
                EqualityComparerExpression<Empty>.Default,
                EqualityComparerExpression<TResult>.Default,
                null,
                null,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                null)
            )
        {
            Contract.Requires(fop != null);
            this.pool = MemoryManager.GetMemoryPool<Empty, TResult>(false);
            this.pool.Get(out this.output);
            this.output.Allocate();

            var fwindow = fop.Compile(0, fop.Size * Config.FuseFactor);
            //Config.DataGranularity = fwindow.Size;
            owindow = new OutputFWindow<TResult>(fwindow);
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            this.Observer = observer;
            owindow.SetBatch(this.output);
            if (owindow.Init())
            {
                int len = 0;
                do
                {
                    len = owindow.Compute();
                    if (this.output.Count > Config.DataBatchSize - owindow.Length)
                    {
                        FlushContents();
                        owindow.SetBatch(this.output);
                    }
                } while (owindow.Slide(owindow.SyncTime));

                FlushContents();
            }

            observer.OnCompleted();
            return default;
        }

        private void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override string ToString() => "FEnd";
    }
}