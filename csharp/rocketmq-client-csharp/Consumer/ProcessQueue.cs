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
using System.Collections.Generic;
using System.Linq;
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
            Console.WriteLine($"FetchMessageImmediately{DateTime.Now}");
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
                
                await ConsumeMessages(messageResult);
                ReceiveMessage();
            }
            catch (Exception e)
            {
                Logger.Error(e, $"[Bug] Exception raised while handling receive result, clientId={_pushConsumer.GetClientId()})");
                ReceiveMessageLater(_pushConsumer._pushSubscriptionSettings.GetLongPollingTimeout());
            }
        }

        private async Task ConsumeMessages(ReceiveMessageResult messageResult)
        {
            if (_pushConsumer._pushSubscriptionSettings.IsFifo())
                //按顺序消费
                await FifoConsumeMessages(messageResult.Messages);
            StandardConsumeMessages(messageResult.Messages);
        }

        /// <summary>
        /// 执行消费
        /// </summary>
        /// <param name="item"></param>
        private async Task Consume(MessageView item)
        {
            try
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
            catch (Exception e)
            {
                // TODO 存在处理异常的问题 暂时不做处理，让消息重新拉取消费
                
               Logger.Error(e, "Failed to consume message");
            }
        }

        /// <summary>
        /// 顺序消息处理
        /// </summary>
        /// <param name="messageResult"></param>
        private async Task FifoConsumeMessages(List<MessageView> messageResult)
        {
            foreach (var item in messageResult.Where(item => !item.IsCorrupted()))
            {
                await ConsumeIteratively(item);
            }
        }

        /// <summary>
        /// 顺序消息消费
        /// </summary>
        /// <param name="item"></param>
        private async Task ConsumeIteratively(MessageView item)
        {
            var result = ConsumeResult.FAILURE;
            try
            {
                result = await _pushConsumer._messageListener.Consume(item).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failed to consume message");
            }

            await EraseFifoMessage(item, result);
        }

        /// <summary>
        /// 顺序消息消费 支持重试
        /// 重试超过次数直接转发到死信队列
        /// </summary>
        /// <param name="messageView"></param>
        /// <param name="consumeResult"></param>
        private async Task EraseFifoMessage(MessageView messageView,ConsumeResult consumeResult)
        {
            var retryPolicy = _pushConsumer._pushSubscriptionSettings.GetRetryPolicy();
            var maxAttempts = retryPolicy.GetMaxAttempts();
            int attempt = messageView.DeliveryAttempt;
            
            switch (consumeResult)
            {
                case ConsumeResult.FAILURE when attempt < maxAttempts:
                {
                    var nextAttemptDelay = retryPolicy.GetNextAttemptDelay(attempt);
                    messageView.AddDeliveryAttempt();

                    await SetTimeout(async () =>
                    {
                        await ConsumeIteratively(messageView);
                    }, nextAttemptDelay);
                    break;
                }
                case ConsumeResult.SUCCESS:
                    await _pushConsumer.Ack(messageView).ConfigureAwait(false);
                    break;
                default:
                    await _pushConsumer.ForwardToDeadLetterQueue(messageView).ConfigureAwait(false);
                    break;
            }
        }
        

        /// <summary>
        /// 延迟执行
        /// </summary>
        /// <param name="action"></param>
        /// <param name="delay"></param>
        private async Task SetTimeout(Func<Task> action, TimeSpan delay)
        {
            await Task.Delay(delay);
            await action();
        }
        
        /// <summary>
        /// 延迟执行
        /// </summary>
        /// <param name="action"></param>
        /// <param name="delay"></param>
        private async Task SetTimeout(Action action, TimeSpan delay)
        {
            await Task.Delay(delay); 
            action();
        }


        /// <summary>
        ///  普通消息处理
        /// </summary>
        /// <param name="messageResult"></param>
        private void StandardConsumeMessages(List<MessageView> messageResult)
        {
            foreach (var item in messageResult.Where(item => !item.IsCorrupted()))
            {
                _ = Consume(item);
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
            SetTimeout(ReceiveMessage, delay).GetAwaiter().GetResult();
        }
    }
}