/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using ServiceUtilities;
using Newtonsoft.Json;

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
    }
}