/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Linq;
using CADFileService.Endpoints.Common;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME_MODEL_ID
    //Note: Custom Procedures microservice uses this class' json structure for decoding the relevant pub/sub messages.
    public class ModelDBEntry
    {
        public static string DBSERVICE_MODELS_TABLE() { return "models-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_MODEL_ID = "modelId";

        public const string MODEL_OWNER_USER_ID_PROPERTY = "modelOwnerUserId";
        public const string MODEL_SHARED_WITH_USER_IDS_PROPERTY = "modelSharedWithUserIds";
        public const string MODEL_UNIQUE_NAME_PROPERTY = "modelUniqueName";
        public const string MODEL_COMMENTS_PROPERTY = "modelComments";
        public const string MODEL_METADATA_PROPERTY = "modelMetadata";
        public const string MODEL_REVISIONS_PROPERTY = "modelRevisions";
        public const string MODEL_CREATION_TIME_PROPERTY = "modelCreationTime";
        public const string MRV_LAST_UPDATE_TIME_PROPERTY = "mrvLastUpdateTime";

        //All fields
        public static readonly string[] Properties =
        {
            MODEL_OWNER_USER_ID_PROPERTY,
            MODEL_SHARED_WITH_USER_IDS_PROPERTY,
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_COMMENTS_PROPERTY,
            MODEL_METADATA_PROPERTY,
            MODEL_REVISIONS_PROPERTY,
            MODEL_CREATION_TIME_PROPERTY,
            MRV_LAST_UPDATE_TIME_PROPERTY
        };

        //Update model info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_METADATA_PROPERTY,
            MODEL_COMMENTS_PROPERTY
        };

        public static readonly Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck = new Dictionary<string, Func<JToken, bool>>()
        {
            [MODEL_UNIQUE_NAME_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [MODEL_METADATA_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                if ((_Parameter as JArray).Count == 0) return true;
                foreach (var Cur in _Parameter)
                {
                    var AsObject = Cur as JObject;
                    if (Cur.Type != JTokenType.Object) return false;
                    foreach (var KeyVal in AsObject)
                        if (!Metadata.Properties.Contains(KeyVal.Key)) return false;
                    foreach (var MetadataMHProp in Metadata.MustHaveProperties)
                        if (!AsObject.ContainsKey(MetadataMHProp)) return false;
                }
                return true;
            },
            [MODEL_COMMENTS_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.String) return false;
                return true;
            }
        };

        public void Prune_NonGettableProperties()
        {
            foreach (var Rev in ModelRevisions)
            {
                Rev.Prune_NonGettableProperties();
            }
        }

        //For creating a new model; these properties should also exist in UpdatableProperties
        public static readonly string[] MustHaveProperties =
        {
            MODEL_UNIQUE_NAME_PROPERTY
        };

        [JsonProperty(MODEL_OWNER_USER_ID_PROPERTY)]
        public string ModelOwnerUserID = "";

        [JsonProperty(MODEL_SHARED_WITH_USER_IDS_PROPERTY)]
        public List<string> ModelSharedWithUserIDs = new List<string>();

        [JsonProperty(MODEL_UNIQUE_NAME_PROPERTY)]
        public string ModelName = "";

        [JsonProperty(MODEL_COMMENTS_PROPERTY)]
        public List<string> ModelComments = new List<string>();

        [JsonProperty(MODEL_REVISIONS_PROPERTY)]
        public List<Revision> ModelRevisions = new List<Revision>();

        [JsonProperty(MODEL_METADATA_PROPERTY)]
        public List<Metadata> ModelMetadata = new List<Metadata>();

        [JsonProperty(MODEL_CREATION_TIME_PROPERTY)]
        public string CreationTime = CommonMethods.GetTimeAsCreationTime();

        [JsonProperty(MRV_LAST_UPDATE_TIME_PROPERTY)]
        public string MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<ModelDBEntry>(_Content.ToString());

            if (_Content.ContainsKey(MODEL_OWNER_USER_ID_PROPERTY))
                ModelOwnerUserID = ContentObject.ModelOwnerUserID;
            if (_Content.ContainsKey(MODEL_SHARED_WITH_USER_IDS_PROPERTY))
                ModelSharedWithUserIDs = ContentObject.ModelSharedWithUserIDs;
            if (_Content.ContainsKey(MODEL_UNIQUE_NAME_PROPERTY))
                ModelName = ContentObject.ModelName;
            if (_Content.ContainsKey(MODEL_COMMENTS_PROPERTY))
                ModelComments = ContentObject.ModelComments;
            if (_Content.ContainsKey(MODEL_REVISIONS_PROPERTY))
                ModelRevisions = ContentObject.ModelRevisions;
            if (_Content.ContainsKey(MODEL_METADATA_PROPERTY))
                ModelMetadata = ContentObject.ModelMetadata;
            if (_Content.ContainsKey(MODEL_CREATION_TIME_PROPERTY))
                CreationTime = ContentObject.CreationTime;
            if (_Content.ContainsKey(MRV_LAST_UPDATE_TIME_PROPERTY))
                MRVLastUpdateTime = ContentObject.MRVLastUpdateTime;
        }
    }
}