/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using rmq = Apache.Rocketmq.V2;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using NLog;


namespace Org.Apache.Rocketmq
{
    public class PushConsumer : Consumer
    {
        private readonly ConcurrentDictionary<string /* topic */, SubscriptionLoadBalancer> _subscriptionRouteDataCache = new ConcurrentDictionary<string, SubscriptionLoadBalancer>();
        private readonly ConcurrentDictionary<string /* topic */, FilterExpression> _subscriptionExpressions;
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly PushSubscriptionSettings _pushSubscriptionSettings;
        private readonly string _consumerGroup;
        private IMessageListener _messageListener;
        private CancellationTokenSource _scanAssignmentCTS;
        private ConcurrentDictionary<string, List<rmq::Assignment>> _topicAssignmentsMap;
        private ConcurrentDictionary<rmq::Assignment, ProcessQueue> _processQueueMap;
        private CancellationTokenSource _scanExpiredProcessQueueCTS;
        private readonly int _maxCacheMessageCount;

        public PushConsumer(ClientConfig clientConfig, string consumerGroup, int maxCacheMessageCount, ConcurrentDictionary<string, FilterExpression> subscriptionExpressions, IMessageListener messageListener) : base(clientConfig, consumerGroup)
        {
            _maxCacheMessageCount = maxCacheMessageCount;
            _subscriptionExpressions = subscriptionExpressions;
            _messageListener = messageListener;
            _consumerGroup = consumerGroup;
            _pushSubscriptionSettings = new PushSubscriptionSettings(ClientId, Endpoints, consumerGroup,clientConfig.RequestTimeout, _subscriptionExpressions);
            
            _topicAssignmentsMap = new ConcurrentDictionary<string, List<rmq::Assignment>>();
            _processQueueMap = new ConcurrentDictionary<rmq::Assignment, ProcessQueue>();
            _scanAssignmentCTS = new CancellationTokenSource();
            _scanExpiredProcessQueueCTS = new CancellationTokenSource();
        }

        public async Task DoStart() => await Start();

        protected override async Task Start()
        {
            if (null == _messageListener)
            {
                throw new System.Exception("Bad configuration: message listener is required");
            }

            await base.Start();

            // Step-1: Resolve topic routes
            List<Task<TopicRouteData>> queryRouteTasks = new List<Task<TopicRouteData>>();
            foreach (var item in _subscriptionExpressions)
            {
                queryRouteTasks.Add(GetRouteData(item.Key));
            }
            await Task.WhenAll(queryRouteTasks);
            

            // Step-2: Scan load assignments that are assigned to current client
            Schedule(async () =>
            {
                await ScanLoadAssignments();
            }, 10, _scanAssignmentCTS.Token);

            Schedule(() =>
            {
                ScanExpiredProcessQueue();
            }, 10, _scanExpiredProcessQueueCTS.Token);
        }
        
        private void Schedule(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                // TODO: log warning
                return;
            }

            while (!token.IsCancellationRequested)
            {
                action();
                Thread.Sleep(seconds * 1000);
            }
        }

        protected override async Task Shutdown()
        {
            _scanAssignmentCTS.Cancel();
            _scanExpiredProcessQueueCTS.Cancel();

            // Shutdown resources of derived class
            await base.Shutdown();
        }

        private async Task ScanLoadAssignments()
        {
            List<Task<List<rmq::Assignment>>> tasks = new List<Task<List<rmq::Assignment>>>();
            foreach (var item in _subscriptionExpressions)
            {
                tasks.Add(ScanLoadAssignment(item.Key, _consumerGroup));
            }
            var result = await Task.WhenAll(tasks);

            foreach (var assignments in result)
            {
                if (assignments.Count == 0)
                    continue;

                CheckAndUpdateAssignments(assignments);
            }
        }
        
        /// <summary>
        /// 查询当前消费者的主题分配的路由信息，返回的分配结果由服务器端负载均衡器决定
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<List<rmq::Assignment>> ScanLoadAssignment(string topic, string group)
        {
            // Pick a broker randomly
            // string target = FilterBroker((s) => true);
            var request = new rmq::QueryAssignmentRequest
            {
                Topic = new rmq.Resource()
                {
                    Name = topic
                },
                Group = new rmq::Resource
                {
                    Name = group
                },
                Endpoints = Endpoints.ToProtobuf()
            };
          
            try
            {
                Console.WriteLine("Start to scan load assignments from server");
                var invocation = await ClientManager.QueryAssignment(Endpoints, request, ClientConfig.RequestTimeout);
                var code = invocation.Response.Status.Code;
                if (!rmq.Code.Ok.Equals(code))
                {
                    throw new Exception($"Failed to query load assignment from server. Cause: {invocation.Response.Status.Message}");
                }

                Console.WriteLine(code);
                return invocation.Response.Assignments.ToList();
            }
            catch (System.Exception e)
            {
                Logger.Warn(e, $"Failed to acquire load assignments for topic {topic} and group {group}");
            }
            // Just return an empty list.
            return new List<rmq.Assignment>();
        }
        
        
        private void ScanExpiredProcessQueue()
        {
            foreach (var item in _processQueueMap)
            {
                if (item.Value.Expired())
                {
                    Task.Run(async () =>
                    {
                        await ExecutePop0(item.Key);
                    });
                }
            }
        }

        private void CheckAndUpdateAssignments(List<rmq::Assignment> assignments)
        {
            if (assignments.Count == 0)
                return;

            string topic = assignments[0].MessageQueue.Topic.Name;

            // Compare to generate or cancel pop-cycles
            List<rmq::Assignment> existing;
            _topicAssignmentsMap.TryGetValue(topic, out existing);

            foreach (var assignment in assignments)
            {
                if (null == existing || !existing.Contains(assignment))
                {
                    ExecutePop(assignment);
                }
            }

            if (null != existing)
            {
                foreach (var assignment in existing)
                {
                    if (!assignments.Contains(assignment))
                    {
                        // Logger.Info($"Stop receiving messages from {assignment.MessageQueue.ToString()}");
                        CancelPop(assignment);
                    }
                }
            }

        }

        private void ExecutePop(rmq::Assignment assignment)
        {
            var processQueue = new ProcessQueue();
            if (_processQueueMap.TryAdd(assignment, processQueue))
            {
                Task.Run(async () =>
                {
                    await ExecutePop0(assignment);
                });
            }
        }

        private async Task ExecutePop0(rmq::Assignment assignment)
        {
            // Logger.Info($"Start to pop {assignment.MessageQueue.ToString()}");
            while (true)
            {
                try
                {
                    ProcessQueue processQueue;
                    if (!_processQueueMap.TryGetValue(assignment, out processQueue))
                    {
                        break;
                    }

                    if (processQueue.Dropped)
                    {
                        break;
                    }
                    var request = WrapReceiveMessageRequest(GetReceptionBatchSize(),new MessageQueue(assignment.MessageQueue), _subscriptionExpressions[assignment.MessageQueue.Topic.Name], _pushSubscriptionSettings.GetLongPollingTimeout());
                    
                    var messageResult = await base.ReceiveMessage(request, new MessageQueue(assignment.MessageQueue), _pushSubscriptionSettings.GetLongPollingTimeout());
                    processQueue.LastReceiveTime = System.DateTime.UtcNow;

                    // TODO: cache message and dispatch them 
                   
                    foreach (var item in messageResult.Messages)
                    {
                        var consumerResult = await _messageListener.Consume(item).ConfigureAwait(false);
                        
                        Console.WriteLine($"Received messages from {item.MessageId}, result={consumerResult}");

                        if (consumerResult== ConsumeResult.SUCCESS)
                            await Ack(item).ConfigureAwait(false);

                        else
                        {
                            var invisibleDuration = _pushSubscriptionSettings.GetRetryPolicy().GetNextAttemptDelay(item.DeliveryAttempt);
                            
                            Console.WriteLine(invisibleDuration);
                            await ChangeInvisibleDuration(item, TimeSpan.FromSeconds(1));
                        }
                    }
                }
                catch (System.Exception e)
                {
                    Logger.Error(e,"Failed to pop messages from broker");
                }
            }
        }
        
        private async Task ChangeInvisibleDuration(MessageView messageView, TimeSpan invisibleDuration)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("push consumer is not running");
            }

            var request = WrapChangeInvisibleDuration(messageView, invisibleDuration);
            var invocation = await ClientManager.ChangeInvisibleDuration(messageView.MessageQueue.Broker.Endpoints,
                request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }
        
        private rmq.ChangeInvisibleDurationRequest WrapChangeInvisibleDuration(MessageView messageView,
            TimeSpan invisibleDuration)
        {
            var topicResource = new rmq.Resource
            {
                Name = messageView.Topic
            };
            return new rmq.ChangeInvisibleDurationRequest
            {
                Topic = topicResource,
                Group = GetProtobufGroup(),
                ReceiptHandle = messageView.ReceiptHandle,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration),
                MessageId = messageView.MessageId
            };
        }
        
        private async Task Ack(MessageView messageView)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("push consumer is not running");
            }

            var request = WrapAckMessageRequest(messageView);
            var invocation = await ClientManager.AckMessage(messageView.MessageQueue.Broker.Endpoints, request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }
        
        private rmq.AckMessageRequest WrapAckMessageRequest(MessageView messageView)
        {
            var topicResource = new rmq.Resource
            {
                Name = messageView.Topic
            };
            var entry = new rmq.AckMessageEntry
            {
                MessageId = messageView.MessageId,
                ReceiptHandle = messageView.ReceiptHandle,
            };
            return new rmq.AckMessageRequest
            {
                Group = GetProtobufGroup(),
                Topic = topicResource,
                Entries = { entry }
            };
        }

        private void CancelPop(rmq::Assignment assignment)
        {
            if (!_processQueueMap.ContainsKey(assignment))
            {
                return;
            }

            ProcessQueue processQueue;
            if (_processQueueMap.Remove(assignment, out processQueue))
            {
                processQueue.Dropped = true;
            }
        }
        
        
        
        
        
        protected override rmq.NotifyClientTerminationRequest WrapNotifyClientTerminationRequest()
        {
            return new rmq.NotifyClientTerminationRequest()
            {
                Group = GetProtobufGroup()
            };
        }

        internal override Settings GetSettings()
        {
            return _pushSubscriptionSettings;
        }

        protected override IEnumerable<string> GetTopics()
        {
            return _subscriptionExpressions.Keys;
        }
        
        protected override rmq.HeartbeatRequest WrapHeartbeatRequest()
        {
            return new rmq::HeartbeatRequest
            {
                ClientType = rmq.ClientType.SimpleConsumer,
                Group = GetProtobufGroup()
            };
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
            UpdateSubscriptionLoadBalancer(topic, topicRouteData);
        }
        
        private SubscriptionLoadBalancer UpdateSubscriptionLoadBalancer(string topic, TopicRouteData topicRouteData)
        {
            if (_subscriptionRouteDataCache.TryGetValue(topic, out var subscriptionLoadBalancer))
            {
                subscriptionLoadBalancer = subscriptionLoadBalancer.Update(topicRouteData);
            }
            else
            {
                subscriptionLoadBalancer = new SubscriptionLoadBalancer(topicRouteData);
            }

            _subscriptionRouteDataCache[topic] = subscriptionLoadBalancer;
            return subscriptionLoadBalancer;
        }

        
        
        private rmq.Resource GetProtobufGroup()
        {
            return new rmq.Resource()
            {
                Name = ConsumerGroup
            };
        }


        private int CacheMessageCountThresholdPerQueue()
        {
            var size = _processQueueMap.Count;
            // All process queues are removed, no need to cache messages.
            if (size <= 0)
                return 0;
            return Math.Max(1, _maxCacheMessageCount / size);
        }

        private int GetReceptionBatchSize()
        {
            return Math.Min(CacheMessageCountThresholdPerQueue(), _pushSubscriptionSettings.GetReceiveBatchSize());
        }
        
    }

}