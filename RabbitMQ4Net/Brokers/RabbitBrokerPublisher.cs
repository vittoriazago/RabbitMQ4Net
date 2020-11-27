using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ4Net.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQ4Net
{
    public class RabbitBrokerPublisher : RabbitBrokerBase
    {
        public RabbitBrokerPublisher(RabbitMqSettings rabbitMqSettings,
            bool globalChannel = true,
            Action<string, Exception> logger = null) : base(rabbitMqSettings, globalChannel, logger)
        {
        }

        public void PublishInQueue<T>(T @event, QueueMqSettings queueSettings)
        {
            var channel = GetChannel();

            string message = JsonConvert.SerializeObject(@event);

            channel.BasicPublish(queueSettings.ExchangeName,
                                 queueSettings.RoutingKey,
                                 false,
                                 null,
                                 Encoding.UTF8.GetBytes(message));
        }

        ConcurrentDictionary<ulong, string> outstandingConfirms = new ConcurrentDictionary<ulong, string>();

        public void PublishInQueue<T>(List<T> events, QueueMqSettings queueSettings)
        {
            var channel = GetChannel();

            channel.BasicAcks += (sender, ea) => CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
            channel.BasicNacks += (sender, ea) => CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);

            foreach (var @event in events)
            {
                string message = JsonConvert.SerializeObject(@event);

                var sequenceNumber = channel.NextPublishSeqNo;
                outstandingConfirms.TryAdd(channel.NextPublishSeqNo, message);

                channel.BasicPublish(queueSettings.ExchangeName,
                                     queueSettings.RoutingKey,
                                     false,
                                     null,
                                     Encoding.UTF8.GetBytes(message));
            }
        }

        private void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
        {
            if (multiple)
            {
                var confirmed = outstandingConfirms.Where(k => k.Key <= sequenceNumber);
                foreach (var entry in confirmed)
                {
                    outstandingConfirms.TryRemove(entry.Key, out _);
                }
            }
            else
            {
                outstandingConfirms.TryRemove(sequenceNumber, out _);
            }
        }

    }
}