using System;
using System.Collections.Generic;
using System.Text;

namespace CADProcessService.Endpoints.Structures
{
    public class OptimizationPresetEntry
    {
        public string LodParameters { get; set; }
        public string CullingThresholds { get; set; }
        public string DistanceThresholds { get; set; }
    }
}
