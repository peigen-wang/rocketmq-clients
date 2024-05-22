using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples;

internal static class PushConsumerExample
{
    private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

    internal static async Task QuickStart()
    {
        // Enable the switch if you use .NET Core 3.1 and want to disable TLS/SSL.
        // AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        const string accessKey = "";
        const string secretKey = "";

        // Credential provider is optional for client configuration.
        var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
        const string endpoints = "192.168.0.39:8081";
        var clientConfig = new ClientConfig.Builder()
            .SetEndpoints(endpoints)
            // .SetCredentialsProvider(credentialsProvider)
            .Build();

        // Add your subscriptions.
        const string consumerGroup = "local-GROUP_PAYMENT_CENTER";
        const string topic = "local-TOPIC_PAYMENT_PAY";
        var subscription = new Dictionary<string, FilterExpression>
            { { topic, new FilterExpression("*") } };
        // In most case, you don't need to create too many consumers, single pattern is recommended.
        var pushConsumer = new PushConsumer(clientConfig, consumerGroup, 1, new ConcurrentDictionary<string, FilterExpression>(subscription), new TestMessageListener());
        pushConsumer.DoStart().GetAwaiter().GetResult();
        
    }



    public class TestMessageListener : IMessageListener
    {
        public async Task<ConsumeResult> Consume(MessageView messages)
        {
            Console.WriteLine($"Received a message, topic={messages.Topic}, message-id={messages.MessageId}, body-size={messages.Body.Length}");
            return await Task.FromResult(ConsumeResult.FAILURE);
        }
    }
}