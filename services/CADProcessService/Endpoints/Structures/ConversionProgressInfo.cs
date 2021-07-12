/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADProcessService.Endpoints.Structures
{
    public class ConversionProgressInfo
    {
        public string ProcessId { get; set; }
        public string VMId { get; set; }
        public int NotificationType { get; set; }
        public int ProcessStatus { get; set; }
        public string Info { get; set; }
        public string Error { get; set; }
        public bool ProcessFailed { get; set; }
        public string ConversionId { get; set; }

        public FileConversionProgressData ProgressDetails;
    }
}
