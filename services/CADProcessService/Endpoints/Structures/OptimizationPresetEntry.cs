/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADProcessService.Endpoints.Structures
{
    public class OptimizationPresetEntry
    {
        public string LodParameters { get; set; }
        public string CullingThresholds { get; set; }
        public string DistanceThresholds { get; set; }
    }
}
