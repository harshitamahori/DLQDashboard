using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using FetchDLQMessages.Models;

namespace FetchDLQMessages.Functions
{
    public class ResubmitToMainQueue
    {
        private readonly ILogger<ResubmitToMainQueue> _logger;

        public ResubmitToMainQueue(ILogger<ResubmitToMainQueue> logger)
        {
            _logger = logger;
        }

        [Function("ResubmitToMainQueue")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            _logger.LogInformation("ResubmitToMainQueue function triggered.");

            
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var payload = JsonSerializer.Deserialize<ResubmitRequest>(requestBody);

            if (payload == null || string.IsNullOrEmpty(payload.Body))
            {
                var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync("Invalid request payload.");
                return badResponse;
            }

            var connectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
            var queueName = Environment.GetEnvironmentVariable("queueName");

            await using var client = new ServiceBusClient(connectionString);

            var sender = client.CreateSender(queueName);

            var message = new ServiceBusMessage(payload.Body)
            {
                MessageId = payload.MessageId 
            };

            await sender.SendMessageAsync(message);

            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteStringAsync("Message resubmitted to main queue successfully.");
            return response;
        }
    }
}
