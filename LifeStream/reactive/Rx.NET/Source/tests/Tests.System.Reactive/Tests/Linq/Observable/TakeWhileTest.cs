﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information. 

using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.Reactive.Testing;
using ReactiveTests.Dummies;
using Xunit;

namespace ReactiveTests.Tests
{
    public class TakeWhileTest : ReactiveTest
    {

        [Fact]
        public void TakeWhile_ArgumentChecking()
        {
            ReactiveAssert.Throws<ArgumentNullException>(() => ((IObservable<int>)null).TakeWhile(DummyFunc<int, bool>.Instance));
            ReactiveAssert.Throws<ArgumentNullException>(() => DummyObservable<int>.Instance.TakeWhile(default(Func<int, bool>)));
            ReactiveAssert.Throws<ArgumentNullException>(() => DummyObservable<int>.Instance.TakeWhile(DummyFunc<int, bool>.Instance).Subscribe(null));
            ReactiveAssert.Throws<ArgumentNullException>(() => ((IObservable<int>)null).TakeWhile((x, i) => true));
            ReactiveAssert.Throws<ArgumentNullException>(() => DummyObservable<int>.Instance.TakeWhile(default(Func<int, int, bool>)));
        }

        [Fact]
        public void TakeWhile_Complete_Before()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnCompleted<int>(330),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                })
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnCompleted<int>(330)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 330)
            );

            Assert.Equal(4, invoked);
        }

        [Fact]
        public void TakeWhile_Complete_After()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                })
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnCompleted<int>(390)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 390)
            );

            Assert.Equal(6, invoked);
        }

        [Fact]
        public void TakeWhile_Error_Before()
        {
            var scheduler = new TestScheduler();

            var ex = new Exception();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnError<int>(270, ex),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                })
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnError<int>(270, ex)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 270)
            );

            Assert.Equal(2, invoked);
        }

        [Fact]
        public void TakeWhile_Error_After()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnError<int>(600, new Exception())
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                })
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnCompleted<int>(390)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 390)
            );

            Assert.Equal(6, invoked);
        }

        [Fact]
        public void TakeWhile_Dispose_Before()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                }),
                300
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 300)
            );

            Assert.Equal(3, invoked);
        }

        [Fact]
        public void TakeWhile_Dispose_After()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                }),
                400
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnCompleted<int>(390)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 390)
            );

            Assert.Equal(6, invoked);
        }

        [Fact]
        public void TakeWhile_Zero()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    return IsPrime(x);
                }),
                300
            );

            res.Messages.AssertEqual(
                OnCompleted<int>(205)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 205)
            );

            Assert.Equal(1, invoked);
        }

        [Fact]
        public void TakeWhile_Throw()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var invoked = 0;
            var ex = new Exception();

            var res = scheduler.Start(() =>
                xs.TakeWhile(x =>
                {
                    invoked++;
                    if (invoked == 3)
                    {
                        throw ex;
                    }

                    return IsPrime(x);
                })
            );

            res.Messages.AssertEqual(
                OnNext(210, 2),
                OnNext(260, 5),
                OnError<int>(290, ex)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 290)
            );

            Assert.Equal(3, invoked);
        }

        [Fact]
        public void TakeWhile_Index1()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnNext(410, 17),
                OnNext(450, 8),
                OnNext(500, 23),
                OnCompleted<int>(600)
            );

            var res = scheduler.Start(() =>
                xs.TakeWhile((x, i) => i < 5)
            );

            res.Messages.AssertEqual(
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnCompleted<int>(350)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 350)
            );
        }

        [Fact]
        public void TakeWhile_Index2()
        {
            var scheduler = new TestScheduler();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnCompleted<int>(400)
            );

            var res = scheduler.Start(() =>
                xs.TakeWhile((x, i) => i >= 0)
            );

            res.Messages.AssertEqual(
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnCompleted<int>(400)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 400)
            );
        }

        [Fact]
        public void TakeWhile_Index_Throw()
        {
            var scheduler = new TestScheduler();

            var ex = new Exception();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnError<int>(400, ex)
            );

            var res = scheduler.Start(() =>
                xs.TakeWhile((x, i) => i >= 0)
            );

            res.Messages.AssertEqual(
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnError<int>(400, ex)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 400)
            );
        }

        [Fact]
        public void TakeWhile_Index_SelectorThrows()
        {
            var scheduler = new TestScheduler();

            var ex = new Exception();

            var xs = scheduler.CreateHotObservable(
                OnNext(90, -1),
                OnNext(110, -1),
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnNext(350, 7),
                OnNext(390, 4),
                OnCompleted<int>(400)
            );

            var res = scheduler.Start(() =>
                xs.TakeWhile((x, i) => { if (i < 5) { return true; } throw ex; })
            );

            res.Messages.AssertEqual(
                OnNext(205, 100),
                OnNext(210, 2),
                OnNext(260, 5),
                OnNext(290, 13),
                OnNext(320, 3),
                OnError<int>(350, ex)
            );

            xs.Subscriptions.AssertEqual(
                Subscribe(200, 350)
            );
        }

        private static bool IsPrime(int i)
        {
            if (i <= 1)
            {
                return false;
            }

            var max = (int)Math.Sqrt(i);
            for (var j = 2; j <= max; ++j)
            {
                if (i % j == 0)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
