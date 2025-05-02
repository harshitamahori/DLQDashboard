using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using System.Text.Json;

namespace FetchDLQMessages.Functions
{
    public class ReceiveAndDeleteDLQ
    {
        private readonly ILogger<ReceiveAndDeleteDLQ> _logger;

        public ReceiveAndDeleteDLQ(ILogger<ReceiveAndDeleteDLQ> logger)
        {
            _logger = logger;
        }

        [Function("ReceiveAndDeleteDLQ")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            _logger.LogInformation("ReceiveAndDeleteDLQ function triggered.");

            var connectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
            var queueName = Environment.GetEnvironmentVariable("queueName");

            await using var client = new ServiceBusClient(connectionString);
            var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            });

            List<object> messageList = new List<object>();

            while (true)
            {
                var messages = await receiver.ReceiveMessagesAsync(maxMessages: 50, maxWaitTime: TimeSpan.FromSeconds(10));

                if (messages == null || !messages.Any())
                {
                    break;
                }

                foreach (var message in messages)
                {
                    messageList.Add(new
                    {
                        message.MessageId,
                        Body = message.Body.ToString(),
                        message.EnqueuedTime,
                        message.SequenceNumber,
                        message.DeadLetterReason,
                        status="Processed",
                        Mode="ReceiveAndDelete"
                    });
                }
            }

            if (messageList.Count == 0)
            {
                var notFoundResponse = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await notFoundResponse.WriteStringAsync("No messages found in DLQ.");
                return notFoundResponse;
            }

            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteStringAsync(JsonSerializer.Serialize(messageList));
            return response;
        }
    }
}
