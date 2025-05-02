using System.Net;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace FetchDLQMessages.Functions
{
    public class CompleteDLQMessage
    {
        private readonly ILogger<CompleteDLQMessage> _logger;
        private readonly IConfiguration _configuration;

        public CompleteDLQMessage(ILogger<CompleteDLQMessage> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        [Function("CompleteDLQMessage")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,FunctionContext executionContext)
        {
            _logger.LogInformation("CompleteDLQMessage function triggered.");

            
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var payload = JsonSerializer.Deserialize<CompleteRequest>(requestBody);

            if (payload == null || string.IsNullOrEmpty(payload.MessageId))
            {
                var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync("Invalid request payload. MessageId is required.");
                return badResponse;
            }

            var connectionString = _configuration.GetConnectionString("ServiceBusConnection");
            var queueName = _configuration.GetValue<string>("AppSettings:QueueName");

            await using var client = new ServiceBusClient(connectionString);
            var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });


            var messages = await receiver.ReceiveMessagesAsync(maxMessages: 10, cancellationToken: CancellationToken.None);

            var targetMessage = messages.FirstOrDefault(m => m.MessageId == payload.MessageId);


            if (targetMessage == null)
            {
                var notFoundResponse = req.CreateResponse(HttpStatusCode.NotFound);
                await notFoundResponse.WriteStringAsync($"Message with ID {payload.MessageId} not found.");
                return notFoundResponse;
            }

            await receiver.CompleteMessageAsync(targetMessage);

            var result = new
            {
                MessageId = targetMessage.MessageId,
                status = "Processed"
            };

            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteAsJsonAsync(result);

            return response;

        }
}

    public class CompleteRequest
    {
        public string MessageId { get; set; }
    }
}

