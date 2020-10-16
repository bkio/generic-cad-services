/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME
    public class AttributeKUPairToOwnerDBEntry : AttributeKeyDBEntryBase
    {
        public static string TABLE() { return "attribute-ku-pairs-" + ServiceUtilities.Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME = "attributeKUPair";
    }
}