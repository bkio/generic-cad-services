/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using CADFileService.Endpoints.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints.Structures
{
    public class Revision
    {
        public const string REVISION_INDEX_PROPERTY = "revisionIndex";
        public const string REVISION_NAME_PROPERTY = "revisionName";
        public const string REVISION_COMMENTS_PROPERTY = "revisionComments";
        public const string REVISION_VERSIONS_PROPERTY = "revisionVersions";
        public const string REVISION_CREATION_TIME_PROPERTY = "revisionCreationTime";

        //Update info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            REVISION_NAME_PROPERTY,
            REVISION_COMMENTS_PROPERTY
        };

        public static readonly Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck = new Dictionary<string, Func<JToken, bool>>()
        {
            [REVISION_NAME_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [REVISION_COMMENTS_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.String) return false;
                return true;
            }
        };
        
        public static readonly string[] MustHaveProperties =
        {
            REVISION_NAME_PROPERTY
        };

        public void Prune_NonGettableProperties()
        {
            foreach (var Ver in RevisionVersions)
            {
                Ver.Prune_NonGettableProperties();
            }
        }

        [JsonProperty(REVISION_INDEX_PROPERTY)]
        public int RevisionIndex = 1;

        [JsonProperty(REVISION_NAME_PROPERTY)]
        public string RevisionName = "";

        [JsonProperty(REVISION_COMMENTS_PROPERTY)]
        public List<string> RevisionComments = new List<string>();

        [JsonProperty(REVISION_CREATION_TIME_PROPERTY)]
        public string CreationTime = CommonMethods.GetTimeAsCreationTime();

        [JsonProperty(REVISION_VERSIONS_PROPERTY)]
        public List<RevisionVersion> RevisionVersions = new List<RevisionVersion>();

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<Revision>(_Content.ToString());

            if (_Content.ContainsKey(REVISION_INDEX_PROPERTY))
                RevisionIndex = ContentObject.RevisionIndex;
            if (_Content.ContainsKey(REVISION_NAME_PROPERTY))
                RevisionName = ContentObject.RevisionName;
            if (_Content.ContainsKey(REVISION_COMMENTS_PROPERTY))
                RevisionComments = ContentObject.RevisionComments;
            if (_Content.ContainsKey(REVISION_CREATION_TIME_PROPERTY))
                RevisionVersions = ContentObject.RevisionVersions;
        }
    }
}