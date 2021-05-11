using System;
using System.Collections.Generic;
using System.Text;

namespace CADProcessService.Endpoints.Structures
{
    public class ConversionProgressInfo
    {
        public string Info { get; set; }
        public int NotificationType { get; set; }

        public bool ProcessFailed { get; set; }

        public FileConversionProgressData ProgressDetails;
    }
}
