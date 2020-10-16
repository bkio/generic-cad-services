/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME_ATTRIBUTE_KEY_VALUE_PAIR_ID
    public class AttributeKVPairUserToOwnerDBEntry : AttributeKeyDBEntryBase
    {
        public static string TABLE() { return "attribute-kvu-pairs-" + ServiceUtilities.Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME = "attributeKVUPair";
    }
}
