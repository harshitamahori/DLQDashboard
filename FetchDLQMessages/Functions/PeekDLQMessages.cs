using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using System.Text.Json;
using System.Linq;

namespace FetchDLQMessages.Functions
{
    public class PeekLockDLQ
    {
        private readonly ILogger<PeekLockDLQ> _logger;

        public PeekLockDLQ(ILogger<PeekLockDLQ> logger)
        {
            _logger = logger;
        }

        [Function("PeekDLQMessages")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
            FunctionContext executionContext)
        {
            _logger.LogInformation("PeekLockDLQ function triggered.");

            var connectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
            var queueName = Environment.GetEnvironmentVariable("queueName");

            await using var client = new ServiceBusClient(connectionString);
            var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

            var messages = await receiver.ReceiveMessagesAsync(maxMessages: 50, maxWaitTime: TimeSpan.FromSeconds(5));

            if (messages == null || !messages.Any())
            {
                var notFoundResponse = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                await notFoundResponse.WriteStringAsync("No messages found in DLQ.");
                return notFoundResponse;
            }

            var result = messages.Select(m => new
            {
                m.MessageId,
                Body = m.Body.ToString(),
                m.EnqueuedTime,
                m.SequenceNumber,
                m.DeadLetterReason,
                Mode = "PeekLock",
                status="Pending"
            }).ToList();

            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteStringAsync(JsonSerializer.Serialize(result));
            return response;
        }
    }
}
