using System;
using System.Collections.Generic;
using System.Text;

namespace CADProcessService.Endpoints.Structures
{
    public class ModelProcessTask
    {
        public string StageDownloadUrl { get; set; }
        public string ModelName { get; set; }
        public int ModelRevision { get; set; }
        public int ProcessStep { get; set; }


        public float GlobalScale { get; set; }
        public float GlobalXOffset { get; set; }
        public float GlobalYOffset { get; set; }
        public float GlobalZOffset { get; set; }

        public float GlobalXRotation { get; set; }
        public float GlobalYRotation { get; set; }
        public float GlobalZRotation { get; set; }

        public float[] LevelThresholds { get; set; }
        public string LodParameters { get; set; }
        public string CullingThresholds { get; set; }
    }
}
