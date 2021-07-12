/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADProcessService.Endpoints.Structures
{
    public class FileConversionProgressData
    {
        public string ModelName { get; set; }
        public int ModelRevision { get; set; }
        public int GlobalCurrentStage { get; set; }
        public int NavisworksTriangleCount { get; set; }
        public int NavisworksTargetChunkCount { get; set; }
        public int NavisworksChunksComplete { get; set; }
        public int[] NavisworksChunkTriangles { get; set; }

        public int PixyzChunksProcessed { get; set; }
        public int PixyzLods { get; set; }
        public int[] PixyzLodTriangles { get; set; }
        public int[] PixyzLodProcessingTime { get; set; }

    }
}
