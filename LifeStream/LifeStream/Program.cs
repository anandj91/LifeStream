using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace LifeStream
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("Hello World!");
            var list = new List<int>();
            for (int i = 0; i < 100; i++)
            {
                list.Add(i);
            }

            list
                .ToObservable()
                .ToTemporalStreamable(e => e, e => e + 1)
                .Select(e => e + 1)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEach(e => Console.WriteLine(e))
                ;
        }
    }
}