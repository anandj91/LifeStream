// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Net.NetworkInformation;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class StitchAggregateStreamable<TKey, TPayload, TState, TResult> : UnaryStreamable<TKey, TPayload, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        internal IAggregate<TPayload, TState, TResult> aggregate;

        public StitchAggregateStreamable(IStreamable<TKey, TPayload> source,
            IAggregate<TPayload, TState, TResult> aggregate)
            : base(source, source.Properties.StitchAggregate(aggregate))
        {
            // This operator uses the equality method on payloads
            if (this.Properties.IsColumnar && !this.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException($"Type of payload, '{typeof(TPayload).FullName}', to Stitch does not have a valid equality operator for columnar mode.");
            }

            this.aggregate = aggregate;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                return new StitchAggregatePipe<TKey, TPayload, TState, TResult>(this, observer);
            }

            var outputType = typeof(PartitionedStitchAggregatePipe<,,,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                part,
                typeof(TState),
                typeof(TResult));
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            /*
             * Columnar Stitching is not supported
             */
            return false;
        }
    }
}