/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME
    public class AttributeKeyToOwnerDBEntry : AttributeKeyDBEntryBase
    {
        public static string TABLE() { return "attribute-keys-" + ServiceUtilities.Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME = "attributeKey";
    }
}
