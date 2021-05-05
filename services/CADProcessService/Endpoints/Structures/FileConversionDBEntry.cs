/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using ServiceUtilities;
using Newtonsoft.Json;
using System;

namespace CADProcessService.Endpoints.Structures
{
    public enum EInternalProcessStage
    {
        Queued,
        Processing,
        ProcessFailed,
        ProcessComplete
    }

    //DB Table entry
    //KeyName = KEY_NAME_CONVERSION_ID
    public class FileConversionDBEntry
    {
        public static string DBSERVICE_FILE_CONVERSIONS_TABLE() { return "file-conversions-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_CONVERSION_ID = "conversionRawRelativeUrl";

        public const string CONVERSION_BUCKET_NAME_PROPERTY = "conversionBucketName";

        public const string CONVERSION_STATUS_PROPERTY = "conversionStatus";

        //All fields
        public static readonly string[] Properties =
        {
            CONVERSION_BUCKET_NAME_PROPERTY,
            CONVERSION_STATUS_PROPERTY
        };

        [JsonProperty(CONVERSION_BUCKET_NAME_PROPERTY)]
        public string BucketName = "";

        [JsonProperty(CONVERSION_STATUS_PROPERTY)]
        public int ConversionStatus = (int)EInternalProcessStage.Queued;
        
        [JsonProperty("queuedTime")]
        public DateTime QueuedTime { get; set; }

        [JsonProperty("modelName")]
        public string ModelName { get; set; }
        [JsonProperty("modelRevision")]
        public int ModelRevision { get; set; }



        [JsonProperty("conversionStage")]
        public int ConversionStage = 0;



        [JsonProperty("globalScale")]
        public float GlobalScale = 1;
        [JsonProperty("globalXOffset")]
        public float GlobalXOffset { get; set; }
        [JsonProperty("globalYOffset")]
        public float GlobalYOffset { get; set; }
        [JsonProperty("globalZOffset")]
        public float GlobalZOffset { get; set; }

        [JsonProperty("globalXRotation")]
        public float GlobalXRotation { get; set; }
        [JsonProperty("globalYRotation")]
        public float GlobalYRotation { get; set; }
        [JsonProperty("globalZRotation")]
        public float GlobalZRotation { get; set; }

        [JsonProperty("levelThresholds")]
        public float[] LevelThresholds { get; set; }
        [JsonProperty("lodParameters")]
        public string LodParameters { get; set; }
        [JsonProperty("cullingThresholds")]
        public string CullingThresholds { get; set; }

    }
}