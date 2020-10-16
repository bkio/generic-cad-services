/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using CADFileService.Endpoints.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints.Structures
{
    public class RevisionVersion
    {
        public const string VERSION_INDEX_PROPERTY = "versionIndex";
        public const string VERSION_NAME_PROPERTY = "versionName";
        public const string VERSION_COMMENTS_PROPERTY = "versionComments";
        public const string VERSION_CREATION_TIME_PROPERTY = "versionCreationTime";
        public const string VERSION_FILE_ENTRY_PROPERTY = "versionFileEntry";

        //Update info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            VERSION_NAME_PROPERTY,
            VERSION_COMMENTS_PROPERTY
        };

        public static readonly Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck = new Dictionary<string, Func<JToken, bool>>()
        {
            [VERSION_NAME_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [VERSION_COMMENTS_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.String) return false;
                return true;
            }
        };
        
        public static readonly string[] MustHaveProperties =
        {
            VERSION_NAME_PROPERTY
        };

        public void Prune_NonGettableProperties()
        {
            FileEntry.Prune_NonGettableProperties();
        }

        [JsonProperty(VERSION_INDEX_PROPERTY)]
        public int VersionIndex = 1;

        [JsonProperty(VERSION_NAME_PROPERTY)]
        public string VersionName = "";

        [JsonProperty(VERSION_COMMENTS_PROPERTY)]
        public List<string> VersionComments = new List<string>();

        [JsonProperty(VERSION_CREATION_TIME_PROPERTY)]
        public string CreationTime = CommonMethods.GetTimeAsCreationTime();

        [JsonProperty(VERSION_FILE_ENTRY_PROPERTY)]
        public FileEntry FileEntry = new FileEntry();

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<RevisionVersion>(_Content.ToString());

            if (_Content.ContainsKey(VERSION_INDEX_PROPERTY))
                VersionIndex = ContentObject.VersionIndex;
            if (_Content.ContainsKey(VERSION_NAME_PROPERTY))
                VersionName = ContentObject.VersionName;
            if (_Content.ContainsKey(VERSION_COMMENTS_PROPERTY))
                VersionComments = ContentObject.VersionComments;
            if (_Content.ContainsKey(VERSION_CREATION_TIME_PROPERTY))
                CreationTime = ContentObject.CreationTime;
            if (_Content.ContainsKey(VERSION_FILE_ENTRY_PROPERTY))
                FileEntry = ContentObject.FileEntry;
        }
    }
}