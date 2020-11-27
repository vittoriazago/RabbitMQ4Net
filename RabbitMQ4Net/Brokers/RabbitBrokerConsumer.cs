using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ4Net.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ4Net
{
    public class RabbitBrokerConsumer : RabbitBrokerBase
    {
        public RabbitBrokerConsumer(RabbitMqSettings rabbitMqSettings,
            bool globalChannel = true,
            Action<string, Exception> logger = null) : base(rabbitMqSettings, globalChannel, logger)
        {
        }

        static readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        public void Subscribe(Func<string, Task<bool>> taskEvent,
            QueueMqSettings queueSettings,
            bool requeue = true)
        {
            var channel = GetChannel();
            channel.BasicQos(0, _rabbitMqSettings.ConsumerPrefetchCount ?? 10, false);

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += async (ch, ea) =>
            {
                try
                {
                    var message = Encoding.UTF8.GetString(ea.Body);
                    var successMessage = await taskEvent(message);

                    if(successMessage)
                        channel.BasicAck(ea.DeliveryTag, false);
                    else
                        channel.BasicNack(ea.DeliveryTag, false, requeue);

                }
                catch (Exception ex)
                {
                    _logger($"Erro ao consumir mensagens, voltando mensagens para fila", ex);
                    channel.BasicNack(ea.DeliveryTag, false, requeue);
                }
            };
            channel.BasicConsume(queueSettings.QueueName, false, consumer);
        }

        public void Subscribe(Func<string, Task<(ulong Tag, bool Success)>> taskEvent, QueueMqSettings queueSettings)
        {
            var channel = GetChannel();
            channel.BasicQos(0, _rabbitMqSettings.ConsumerPrefetchCount ?? 10, false);

            var rawMessages = new List<BasicDeliverEventArgs>(100);
            var consumer = new EventingBasicConsumer(channel);

            int countBatch = 20;
            consumer.Received += async (ch, ea) =>
            {
                try
                {
                    rawMessages.Add(ea);

                    if (rawMessages.Count() == countBatch)
                    {
                        await semaphoreSlim.WaitAsync();

                        var tasks = rawMessages.Select(item =>
                        {
                            var message = Encoding.UTF8.GetString(item.Body);
                            return taskEvent(message);
                        });
                        var results = await Task.WhenAll(tasks);

                        var failedResults = results.Where(r => !r.Success);
                        for (int i = failedResults.Count() - 1; i >= 0; i--)
                        {
                            var (Tag, Success) = failedResults.ElementAt(i);
                            channel.BasicNack(Tag, false, true);
                        }

                        if (results.Any(r => r.Success))
                            channel.BasicAck(results.FirstOrDefault(r => r.Success).Tag, true);

                        rawMessages.Clear();

                        semaphoreSlim.Release();
                    }
                }
                catch (Exception ex)
                {
                    _logger($"Erro ao consumir mensagens, voltando mensagens para fila", ex);
                    channel.BasicNack(rawMessages.FirstOrDefault().DeliveryTag, true, true);

                    rawMessages.Clear();
                }
            };
            channel.BasicConsume(queueSettings.QueueName, false, consumer);
        }

    }
}