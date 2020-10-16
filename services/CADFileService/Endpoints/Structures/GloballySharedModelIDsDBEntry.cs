/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using Newtonsoft.Json;
using ServiceUtilities;

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID
    public class GloballySharedModelIDsDBEntry
    {
        public static string DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE() { return "globally-shared-model-ids-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_MODEL_ID = "modelId";

        public const string EMPTY_PROPERTY = "ignore";

        public static readonly string[] Properties =
        {
            EMPTY_PROPERTY
        };

        [JsonProperty(EMPTY_PROPERTY)]
        public string Empty = "";
    }
}