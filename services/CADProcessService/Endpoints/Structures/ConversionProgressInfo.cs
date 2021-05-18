using System;
using System.Collections.Generic;
using System.Text;

namespace CADProcessService.Endpoints.Structures
{
    public class ConversionProgressInfo
    {
        public Guid ProcessId { get; set; }
        public string VMId { get; set; }
        public int NotificationType { get; set; }
        public int ProcessStatus { get; set; }
        public string Info { get; set; }
        public bool ProcessFailed { get; set; }

        public FileConversionProgressData ProgressDetails;
    }
}
