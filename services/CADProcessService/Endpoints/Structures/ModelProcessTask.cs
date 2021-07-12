/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADProcessService.Endpoints.Structures
{
    public class FilterInfo
    {
        public int FilterType { get; set; }
        public string FilterDefinition { get; set; }
        public string FilterName { get; set; }
    }
    public class ModelProcessTask
    {
        public string ConversionId { get; set; }
        public string StageDownloadUrl { get; set; }
        public string ModelName { get; set; }
        public int ModelRevision { get; set; }
        public int ProcessStep { get; set; }
        public string ZipMainAssemblyFileNameIfAny { get; set; }


        public float GlobalScale { get; set; }
        public float GlobalXOffset { get; set; }
        public float GlobalYOffset { get; set; }
        public float GlobalZOffset { get; set; }

        public float GlobalXRotation { get; set; }
        public float GlobalYRotation { get; set; }
        public float GlobalZRotation { get; set; }
        public string CustomPythonScript { get; set; }

        public string LevelThresholds { get; set; }
        public string LodParameters { get; set; }
        public string CullingThresholds { get; set; }
        //public List<FilterInfo> Filters { get; set; }
        public string Filters { get; set; }
    }
}
