using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;
using NLog;

namespace Org.Apache.Rocketmq;

public class PushSubscriptionSettings : Settings
{
    private readonly Resource _group;
    private readonly ConcurrentDictionary<string /* topic */, FilterExpression> _subscriptionExpressions;
    private volatile bool _fifo = false;
    private int _receiveBatchSize = 32;
    private TimeSpan _longPollingTimeout = TimeSpan.FromSeconds(30);
    private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

    public PushSubscriptionSettings(string clientId, Endpoints endpoints,string consumerGroup, TimeSpan requestTimeout, ConcurrentDictionary<string /* topic */, FilterExpression> subscriptionExpressions) : base(clientId, ClientType.PushConsumer, endpoints, requestTimeout)
    {
        _group = new Resource(consumerGroup);
        _subscriptionExpressions = subscriptionExpressions;
    }
    
    public bool IsFifo() {
        return _fifo;
    }
    
    public int GetReceiveBatchSize() {
        return _receiveBatchSize;
    }

    public TimeSpan GetLongPollingTimeout() {
        return _longPollingTimeout;
    }

    public override global::Apache.Rocketmq.V2.Settings ToProtobuf()
    {
        var subscriptionEntries = new List<Proto.SubscriptionEntry>();
        foreach (var (key, value) in _subscriptionExpressions)
        {
            var topic = new Proto.Resource()
            {
                Name = key,
            };
            var subscriptionEntry = new Proto.SubscriptionEntry();
            var filterExpression = new Proto.FilterExpression();
            switch (value.Type)
            {
                case ExpressionType.Tag:
                    filterExpression.Type = Proto.FilterType.Tag;
                    break;
                case ExpressionType.Sql92:
                    filterExpression.Type = Proto.FilterType.Sql;
                    break;
                default:
                    Logger.Warn($"[Bug] Unrecognized filter type={value.Type} for simple consumer");
                    break;
            }

            filterExpression.Expression = value.Expression;
            subscriptionEntry.Topic = topic;
            subscriptionEntries.Add(subscriptionEntry);
        }

        var subscription = new Proto.Subscription
        {
            Group = _group.ToProtobuf(),
            Subscriptions = { subscriptionEntries },
            LongPollingTimeout = Duration.FromTimeSpan(_longPollingTimeout)
        };
        return new Proto.Settings
        {
            AccessPoint = Endpoints.ToProtobuf(),
            ClientType = ClientTypeHelper.ToProtobuf(ClientType),
            RequestTimeout = Duration.FromTimeSpan(RequestTimeout),
            Subscription = subscription,
            UserAgent = UserAgent.Instance.ToProtobuf()
        };
    }

    public override void Sync(global::Apache.Rocketmq.V2.Settings settings)
    {
        if (Proto.Settings.PubSubOneofCase.Subscription != settings.PubSubCase)
        {
            Logger.Error($"[Bug] Issued settings doesn't match with the client type, clientId={ClientId}, " + $"pubSubCase={settings.PubSubCase}, clientType={ClientType}");
        }

        var subscription = settings.Subscription;
        this._fifo = subscription.Fifo;
        this._receiveBatchSize = subscription.ReceiveBatchSize;
        this._longPollingTimeout = subscription.LongPollingTimeout.ToTimeSpan();
        var backoffPolicy = settings.BackoffPolicy;
        switch (backoffPolicy.StrategyCase) {
            case Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff:
                RetryPolicy = ExponentialBackoffRetryPolicy.FromProtobuf(backoffPolicy);
                break;
            case Proto.RetryPolicy.StrategyOneofCase.CustomizedBackoff:
                RetryPolicy = CustomizedBackoffRetryPolicy.FromProtobuf(backoffPolicy);
                break;
            default:
                throw new ArgumentException("Unrecognized backoff policy strategy.");
        }
    }
}