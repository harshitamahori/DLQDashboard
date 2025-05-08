using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using System.Text.Json;
using System.Linq;


namespace FetchDLQMessages.Functions
{
    public class PeekDLQMessage
    {
        private readonly ILogger<PeekDLQMessage> _logger;
        private readonly IConfiguration _configuration;

        public PeekDLQMessage(ILogger<PeekDLQMessage> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        [Function("PeekDLQMessage")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req)
        {
            _logger.LogInformation("PeekDLQMessage is triggered");

            var connectionString = _configuration.GetConnectionString("ServiceBusConnection");
            var queueName = _configuration.GetValue<string>("AppSettings:QueueName");

            await using var client = new ServiceBusClient(connectionString);
            var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter 
            });

            
            var messages = await receiver.PeekMessagesAsync(maxMessages: 50, fromSequenceNumber:0);

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
            }).ToList();

            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteStringAsync(JsonSerializer.Serialize(result));
            return response;


        }
    }
}
