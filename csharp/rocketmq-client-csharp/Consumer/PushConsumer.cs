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
        public readonly PushSubscriptionSettings _pushSubscriptionSettings;
        private readonly string _consumerGroup;
        public IMessageListener _messageListener;
        private CancellationTokenSource _scanAssignmentCTS;
        private ConcurrentDictionary<string, List<rmq::Assignment>> _topicAssignmentsMap;
        private ConcurrentDictionary<MessageQueue, ProcessQueue> _processQueueMap;
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
            _processQueueMap = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
            _scanAssignmentCTS = new CancellationTokenSource();
            _scanExpiredProcessQueueCTS = new CancellationTokenSource();
        }

        public async Task DoStart() => await Start();

        protected override async Task Start()
        {
            try
            {
                State = State.Starting;
                await base.Start();
                State = State.Running;
                // Step-1: Resolve topic routes
                List<Task<TopicRouteData>> queryRouteTasks = new List<Task<TopicRouteData>>();
                foreach (var item in _subscriptionExpressions)
                {
                    queryRouteTasks.Add(GetRouteData(item.Key));
                }
                await Task.WhenAll(queryRouteTasks);
            

                // Step-2: Scan load assignments that are assigned to current client
                await Schedule(() =>
                {
                    try
                    {
                        // 异步调用 不用等待结果
                        _ = ScanLoadAssignments();
                    }
                    catch (Exception e)
                    {
                        Logger.Error(e, $"Exception raised while scanning the load assignments, clientId={ClientId}");
                    }
                }, 5, _scanAssignmentCTS.Token);

            }
            catch (Exception)
            {
                State = State.Failed;
                await Shutdown();
                throw;
            }
        }
        
        /// <summary>
        /// Schedule a task to run periodically
        /// </summary>
        /// <param name="action"></param>
        /// <param name="seconds"></param>
        /// <param name="token"></param>
        private async Task Schedule(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                // TODO: log warning
                return;
            }

            while (!token.IsCancellationRequested)
            {
                action();
                await Task.Delay(seconds * 1000, token);
            }
        }

        protected override async Task Shutdown()
        {
            _scanAssignmentCTS.Cancel();
            _scanExpiredProcessQueueCTS.Cancel();
            State = State.Stopping;
            // Shutdown resources of derived class
            await base.Shutdown();
            State = State.Terminated;
        }

        private async Task ScanLoadAssignments()
        {
            foreach (var item in _subscriptionExpressions)
            {
                var topic = item.Key;
                _topicAssignmentsMap.TryGetValue(topic, out var existing);
                var assignments = await ScanLoadAssignment(item.Key, _consumerGroup);
                if(assignments==null || !assignments.Any()) continue;

                if (!assignments.Equals(existing))
                {
                    _topicAssignmentsMap.TryAdd(topic, assignments);
                }
                // Process queue may be dropped, need to be synchronized anyway.
                SyncProcessQueue(topic, assignments, item.Value);
            }
        }
        
        private void SyncProcessQueue(string topic, List<rmq::Assignment> assignments, FilterExpression filterExpression)
        {
            if (assignments.Count == 0)
                return;

            var latest = new HashSet<MessageQueue>();
            foreach (var assignment in assignments)
            {
                latest.Add(new MessageQueue(assignment.MessageQueue));
            }

            var activeMqs = new HashSet<MessageQueue>();
            
            foreach (var item in _processQueueMap)
            {
               var mq = item.Key;
               var pq = item.Value;
                if (!topic.Equals(mq.Topic)) {
                    continue;
                }

                if (!latest.Contains(mq))
                {
                    _processQueueMap.Remove(item.Key, out pq);
                    if (pq != null) pq.Dropped = true;
                    continue;
                }

                if (pq.Expired()) {
                    _processQueueMap.Remove(item.Key, out pq);
                    if (pq != null) pq.Dropped = true;
                    continue;
                }
                activeMqs.Add(mq);
            }

            foreach (var mq in latest)
            {
                if (activeMqs.Contains(mq)) {
                    continue;
                }

                var processQueue = new ProcessQueue(this, mq, filterExpression);
                _processQueueMap.TryAdd(mq, processQueue);
                processQueue.FetchMessageImmediately();
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
                var invocation = await ClientManager.QueryAssignment(Endpoints, request, ClientConfig.RequestTimeout);
                var code = invocation.Response.Status.Code;
                if (!rmq.Code.Ok.Equals(code))
                {
                    throw new Exception($"Failed to query load assignment from server. Cause: {invocation.Response.Status.Message}");
                }
                return invocation.Response.Assignments.ToList();
            }
            catch (System.Exception e)
            {
                Logger.Warn(e, $"Failed to acquire load assignments for topic {topic} and group {group}");
            }
            // Just return an empty list.
            return new List<rmq.Assignment>();
        }


        public async Task ChangeInvisibleDuration(MessageView messageView, TimeSpan invisibleDuration)
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

        public async Task Ack(MessageView messageView)
        {
            if (State.Running != State)
                throw new InvalidOperationException("push consumer is not running");

            var request = WrapAckMessageRequest(messageView);
            var invocation = await ClientManager.AckMessage(messageView.MessageQueue.Broker.Endpoints, request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }

        public async Task ForwardToDeadLetterQueue(MessageView messageView)
        {
            if (State.Running != State)
                throw new InvalidOperationException("push consumer is not running");

            var request = WarpForwardMessageToDeadLetterQueueRequest(messageView);
            var invocation = await ClientManager.ForwardMessageToDeadLetterQueue(messageView.MessageQueue.Broker.Endpoints, request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }
        
        private rmq.ForwardMessageToDeadLetterQueueRequest WarpForwardMessageToDeadLetterQueueRequest(MessageView messageView)
        {
            var topicResource = new rmq.Resource
            {
                Name = messageView.Topic
            };
            return new rmq.ForwardMessageToDeadLetterQueueRequest
            {
                Group = GetProtobufGroup(),
                Topic = topicResource,
                ReceiptHandle = messageView.ReceiptHandle,
                MessageId = messageView.MessageId,
                DeliveryAttempt = messageView.DeliveryAttempt,
                MaxDeliveryAttempts = _pushSubscriptionSettings.GetRetryPolicy().GetMaxAttempts()
            };
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


        public int CacheMessageCountThresholdPerQueue()
        {
            var size = _processQueueMap.Count;
            // All process queues are removed, no need to cache messages.
            if (size <= 0)
                return 0;
            return Math.Max(1, _maxCacheMessageCount / size);
        }

        public int GetReceptionBatchSize()
        {
            return Math.Min(CacheMessageCountThresholdPerQueue(), _pushSubscriptionSettings.GetReceiveBatchSize());
        }
        
    }

}