using System;
using System.Collections.Generic;
using System.Linq;
using Proto = Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;

namespace Org.Apache.Rocketmq;

public class CustomizedBackoffRetryPolicy : IRetryPolicy
{
    private readonly int _maxAttempts;
    private List<TimeSpan> _durations;

    private CustomizedBackoffRetryPolicy(List<TimeSpan> durations, int maxAttempts)
    {
        _maxAttempts = maxAttempts;
        _durations= durations;
    }

    
    
    public int GetMaxAttempts()
    {
        return _maxAttempts;
    }

    public IRetryPolicy InheritBackoff(Proto.RetryPolicy retryPolicy)
    {
        if (retryPolicy.StrategyCase != Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff)
        {
            throw new InvalidOperationException("Strategy must be exponential backoff");
        }

        return InheritBackoff(retryPolicy.CustomizedBackoff);
    }

    private IRetryPolicy InheritBackoff(Proto.CustomizedBackoff backoff)
    {
        var durations = backoff.Next.Select(x=>x.ToTimeSpan()).ToList();
        return new CustomizedBackoffRetryPolicy(durations, _maxAttempts);
    }

    public TimeSpan GetNextAttemptDelay(int attempt)
    {
        return attempt > _durations.Count ? _durations.LastOrDefault() : _durations[attempt - 1];
    }

    public global::Apache.Rocketmq.V2.RetryPolicy ToProtobuf()
    {
        var exponentialBackoff = new Proto.CustomizedBackoff
        {
            Next = { _durations.Select(x => x.ToDuration()) }
        };
        return new global::Apache.Rocketmq.V2.RetryPolicy
        {
            MaxAttempts = _maxAttempts,
            CustomizedBackoff = exponentialBackoff
        };
    }
    

    public static IRetryPolicy FromProtobuf(Proto.RetryPolicy backoffPolicy)
    {
        if (backoffPolicy.StrategyCase != Proto.RetryPolicy.StrategyOneofCase.CustomizedBackoff)
        {
            throw new InvalidOperationException("Strategy must be exponential backoff");
        }

        return new CustomizedBackoffRetryPolicy(backoffPolicy.CustomizedBackoff.Next.Select(x=>x.ToTimeSpan()).ToList(), backoffPolicy.MaxAttempts);
    }
}