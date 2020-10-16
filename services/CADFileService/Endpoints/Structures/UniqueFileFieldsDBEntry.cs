/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using ServiceUtilities;

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = ModelDBEntry.MODEL_UNIQUE_NAME_PROPERTY
    public class UniqueFileFieldsDBEntry
    {
        public static string DBSERVICE_UNIQUEFILEFIELDS_TABLE() { return "unique-file-fields-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_MODEL_UNIQUE_NAME = ModelDBEntry.MODEL_UNIQUE_NAME_PROPERTY;

        public static readonly string[] Properties = 
        {
            ModelDBEntry.KEY_NAME_MODEL_ID
        };
    }
}
