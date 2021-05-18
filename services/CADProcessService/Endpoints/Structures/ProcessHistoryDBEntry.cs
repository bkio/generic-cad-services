/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.Common;
using System;

namespace CADProcessService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME_VM_UNIQUE_ID
    public class ProcessHistoryDBEntry
    {
        public static string DBSERVICE_PROCESS_HISTORY_TABLE() { return "process-history-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_PROCESS_ID = "processId";

        public const string MODEL_UNIQUE_NAME_PROPERTY = "modelUniqueName";
        public const string MODEL_REVISION_INDEX_PROPERTY = "revisionIndex";
        public const string HISTORY_RECORD_DATE_PROPERTY = "historyRecordDate";
        public const string CURRENT_PROCESS_STAGE_PROPERTY = "currentProcessStage";
        public const string PROCESS_STATUS_PROPERTY = "processStatus";
        public const string PROCESS_INFO_PROPERTY = "processInfo";

        //All fields
        public static readonly string[] Properties =
        {
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_REVISION_INDEX_PROPERTY,
            HISTORY_RECORD_DATE_PROPERTY,
            CURRENT_PROCESS_STAGE_PROPERTY,
            PROCESS_STATUS_PROPERTY,
            PROCESS_INFO_PROPERTY
        };

        //For creating a new model; these properties should also exist in UpdatableProperties
        public static readonly string[] MustHaveProperties =
        {
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_REVISION_INDEX_PROPERTY,
            CURRENT_PROCESS_STAGE_PROPERTY,
            HISTORY_RECORD_DATE_PROPERTY,
            PROCESS_STATUS_PROPERTY
        };

        [JsonProperty(MODEL_UNIQUE_NAME_PROPERTY)]
        public string ModelName { get; set; }

        [JsonProperty(MODEL_REVISION_INDEX_PROPERTY)]
        public int RevisionIndex { get; set; }

        [JsonProperty(PROCESS_STATUS_PROPERTY)]
        public int ProcessStatus = (int)EProcessStatus.Idle;

        [JsonProperty(HISTORY_RECORD_DATE_PROPERTY)]
        public string HistoryRecordDate = Methods.GetUtcNowShortDateAndLongTimeString();

        [JsonProperty(CURRENT_PROCESS_STAGE_PROPERTY)]
        public int CurrentProcessStage = (int)EProcessStage.Stage0_FileUpload;

        [JsonProperty(PROCESS_INFO_PROPERTY)]
        public string ProcessInfo { get; set; }

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<ProcessHistoryDBEntry>(_Content.ToString());

            if (_Content.ContainsKey(MODEL_UNIQUE_NAME_PROPERTY))
                ModelName = ContentObject.ModelName;
            if (_Content.ContainsKey(MODEL_REVISION_INDEX_PROPERTY))
                RevisionIndex = ContentObject.RevisionIndex;
            if (_Content.ContainsKey(HISTORY_RECORD_DATE_PROPERTY))
                HistoryRecordDate = ContentObject.HistoryRecordDate;
            if (_Content.ContainsKey(CURRENT_PROCESS_STAGE_PROPERTY))
                CurrentProcessStage = ContentObject.CurrentProcessStage;
            if (_Content.ContainsKey(PROCESS_STATUS_PROPERTY))
                ProcessStatus = ContentObject.ProcessStatus;
            if (_Content.ContainsKey(PROCESS_INFO_PROPERTY))
                ProcessInfo = ContentObject.ProcessInfo;
        }
    }
}
