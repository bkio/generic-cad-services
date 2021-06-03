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

        public const string QUEUED_TIME_PROPERTY = "queuedTime";
        public const string UPDATED_TIME_PROPERTY = "updatedTime";
        public const string MODEL_NAME_PROPERTY = "modelName";
        public const string MODEL_REVISION_PROPERTY = "modelRevision";
        public const string CONVERSION_STAGE_PROPERTY = "conversionStage";
        public const string GLOBAL_SCALE_PROPERTY = "globalScale";
        public const string GLOBAL_X_OFFSET_PROPERTY = "globalXOffset";
        public const string GLOBAL_Y_OFFSET_PROPERTY = "globalYOffset";
        public const string GLOBAL_Z_OFFSET_PROPERTY = "globalZOffset";
        public const string GLOBAL_X_ROTATION_PROPERTY = "globalXRotation";
        public const string GLOBAL_Y_ROTATION_PROPERTY = "globalYRotation";
        public const string GLOBAL_Z_ROTATION_PROPERTY = "globalZRotation";
        public const string LEVEL_THRESHOLDS_PROPERTY = "levelThresholds";
        public const string LOD_PARAMETERS_PROPERTY = "lodParameters";
        public const string CULLNG_THRESHOLDS_PROPERTY = "cullingThresholds";
        public const string FILTER_SETTINGS_PROPERTY = "filterSettings";
        public const string DELETE_DUPLICATES_PROPERTY = "deleteDuplicates";
        public const string MERGE_FINAL_LEVEL_PROPERTY = "mergeFinalLevel";
        public const string ZIP_ASSEMBLY_FILENAME_PROPERTY = "zipMainAssemblyFileNameIfAny";

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
        
        [JsonProperty(QUEUED_TIME_PROPERTY)]
        public string QueuedTime { get; set; }

        [JsonProperty(UPDATED_TIME_PROPERTY)]
        public string UpdatedTime { get; set; }

        [JsonProperty(MODEL_NAME_PROPERTY)]
        public string ModelName { get; set; }
        [JsonProperty(MODEL_REVISION_PROPERTY)]
        public int ModelRevision { get; set; }

        [JsonProperty(CONVERSION_STAGE_PROPERTY)]
        public int ConversionStage = 0;
        [JsonProperty(ZIP_ASSEMBLY_FILENAME_PROPERTY)]
        public string ZipMainAssemblyFileNameIfAny { get; set; }


        [JsonProperty(GLOBAL_SCALE_PROPERTY)]
        public float GlobalScale = 0.01f;
        [JsonProperty(GLOBAL_X_OFFSET_PROPERTY)]
        public float GlobalXOffset { get; set; }
        [JsonProperty(GLOBAL_Y_OFFSET_PROPERTY)]
        public float GlobalYOffset { get; set; }
        [JsonProperty(GLOBAL_Z_OFFSET_PROPERTY)]
        public float GlobalZOffset { get; set; }

        [JsonProperty(GLOBAL_X_ROTATION_PROPERTY)]
        public float GlobalXRotation { get; set; }
        [JsonProperty(GLOBAL_Y_ROTATION_PROPERTY)]
        public float GlobalYRotation { get; set; }
        [JsonProperty(GLOBAL_Z_ROTATION_PROPERTY)]
        public float GlobalZRotation { get; set; }

        [JsonProperty(LEVEL_THRESHOLDS_PROPERTY)]
        public float[] LevelThresholds { get; set; }
        [JsonProperty(LOD_PARAMETERS_PROPERTY)]
        public string LodParameters { get; set; }
        [JsonProperty(CULLNG_THRESHOLDS_PROPERTY)]
        public string CullingThresholds { get; set; }
        [JsonProperty(FILTER_SETTINGS_PROPERTY)]
        public string FilterSettings { get; set; }

        [JsonProperty(DELETE_DUPLICATES_PROPERTY)]
        public string DeleteDuplicates { get; set; }

        [JsonProperty(MERGE_FINAL_LEVEL_PROPERTY)]
        public string MergeFinalLevel { get; set; }

    }
}