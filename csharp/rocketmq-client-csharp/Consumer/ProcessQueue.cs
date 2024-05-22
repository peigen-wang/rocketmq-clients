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
using System.Threading.Tasks;
using Apache.Rocketmq.V2;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class ProcessQueue
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public ProcessQueue()
        {
            _lastReceivedTime = DateTime.UtcNow;
        }
        public bool Dropped { get; set; }

        private DateTime _lastReceivedTime;

        public DateTime LastReceiveTime
        {
            get { return _lastReceivedTime; }
            set { _lastReceivedTime = value; }
        }

        internal bool Expired()
        {
            return DateTime.UtcNow.Subtract(_lastReceivedTime).TotalMilliseconds > 30 * 1000;
        }


        private readonly PushConsumer _pushConsumer;
        private readonly MessageQueue _messageQueue;
        private readonly FilterExpression _filterExpression;
        
        public ProcessQueue(PushConsumer pushConsumer, MessageQueue messageQueue, FilterExpression filterExpression)
        {
            _pushConsumer = pushConsumer;
            _messageQueue = messageQueue;
            _filterExpression = filterExpression;
            _lastReceivedTime = DateTime.UtcNow;
        }


        public void FetchMessageImmediately()
        {
            var clientId = _pushConsumer.GetClientId();
            if (_pushConsumer.State != State.Running)
                return;
            try
            {
                
                // 建立长轮询请求 异步获取消息 不阻塞当前线程
                var request = _pushConsumer.WrapReceiveMessageRequest(_pushConsumer.GetReceptionBatchSize(), _messageQueue, _filterExpression, _pushConsumer._pushSubscriptionSettings.GetLongPollingTimeout());
                
                _ = AsyncHandlerMessage(request);
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failed to pop messages from broker");
            }
        }

        private async Task AsyncHandlerMessage(ReceiveMessageRequest request)
        {
            try
            {
                var messageResult = await _pushConsumer.ReceiveMessage(request, _messageQueue, _pushConsumer._pushSubscriptionSettings.GetLongPollingTimeout());
                LastReceiveTime = DateTime.UtcNow;
                
                foreach (var item in messageResult.Messages)
                {
                    var consumerResult = await _pushConsumer._messageListener.Consume(item).ConfigureAwait(false);

                    if (consumerResult == ConsumeResult.SUCCESS)
                    {
                        await _pushConsumer.Ack(item).ConfigureAwait(false);
                    }
                    else
                    {
                        var invisibleDuration = _pushConsumer._pushSubscriptionSettings.GetRetryPolicy().GetNextAttemptDelay(item.DeliveryAttempt);

                        await _pushConsumer.ChangeInvisibleDuration(item, invisibleDuration);
                    }
                }
                ReceiveMessage();
            }
            catch (Exception e)
            {
                Logger.Error(e, $"[Bug] Exception raised while handling receive result, clientId={_pushConsumer.GetClientId()})");
                ReceiveMessageLater(_pushConsumer._pushSubscriptionSettings.GetLongPollingTimeout());
            }
        }

        private void ReceiveMessage()
        {
           var clientId = _pushConsumer.GetClientId();
            if (Dropped) {
                Logger.Info("Process queue has been dropped, no longer receive message, mq={0}, clientId={1}", _messageQueue.QueueId, clientId);
                return;
            }

            FetchMessageImmediately();
        }
        
        private void ReceiveMessageLater(TimeSpan delay)
        {
            Task.Delay(delay).ContinueWith(t => ReceiveMessage());
        }
    }
}