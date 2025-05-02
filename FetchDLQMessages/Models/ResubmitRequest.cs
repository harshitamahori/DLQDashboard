
namespace FetchDLQMessages.Models
{
    public class ResubmitRequest
    {
        public string MessageId { get; set; }
        public string Body { get; set; }
    }
}
