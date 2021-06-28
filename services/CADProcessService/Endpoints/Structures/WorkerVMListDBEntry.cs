﻿/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.Common;

namespace CADProcessService.Endpoints.Structures
{
    //DB Table entry
    //KeyName = KEY_NAME_VM_UNIQUE_ID
    public class WorkerVMListDBEntry
    {
        public static string DBSERVICE_WORKERS_VM_LIST_TABLE() { return "worker-vm-list-" + Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash(); }

        public const string KEY_NAME_VM_UNIQUE_ID = "vmUniqueId";

        public const string VM_NAME_PROPERTY = "vmName";
        public const string VM_STATUS_PROPERTY = "vmStatus";
        public const string MODEL_UNIQUE_NAME_PROPERTY = "modelUniqueName";
        public const string MODEL_REVISION_INDEX_PROPERTY = "revisionIndex";
        public const string PROCESS_ID_PROPERTY = "processId";
        public const string PROCESS_START_DATE_PROPERTY = "processStartDate";
        public const string LAST_KNOWN_PROCESS_STATUS_PROPERTY = "lastKnownProcessStatus";
        public const string LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY = "lastKnownProcessStatusInfo";
        public const string CURRENT_PROCESS_STAGE_PROPERTY = "currentProcessStage";

        //All fields
        public static readonly string[] Properties =
        {
            VM_NAME_PROPERTY,
            VM_STATUS_PROPERTY,
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_REVISION_INDEX_PROPERTY,
            PROCESS_ID_PROPERTY,
            PROCESS_START_DATE_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY,
            CURRENT_PROCESS_STAGE_PROPERTY
        };

        //Update model info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_REVISION_INDEX_PROPERTY,
            PROCESS_ID_PROPERTY,
            PROCESS_START_DATE_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY,
            VM_STATUS_PROPERTY,
            VM_NAME_PROPERTY
        };

        //For creating a new model; these properties should also exist in UpdatableProperties
        public static readonly string[] MustHaveProperties =
        {
            MODEL_UNIQUE_NAME_PROPERTY,
            MODEL_REVISION_INDEX_PROPERTY,
            PROCESS_ID_PROPERTY,
            PROCESS_START_DATE_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_PROPERTY,
            LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY,
            VM_STATUS_PROPERTY,
            VM_NAME_PROPERTY
        };

        [JsonProperty(VM_STATUS_PROPERTY)]
        public int VMStatus = (int)EVMStatus.Available;

        [JsonProperty(VM_NAME_PROPERTY)]
        public string VMName { get; set; }

        [JsonProperty(PROCESS_ID_PROPERTY)]
        public string ProcessId { get; set; }

        [JsonProperty(PROCESS_START_DATE_PROPERTY)]
        public string ProcessStartDate = Methods.ToISOString();

        [JsonProperty(LAST_KNOWN_PROCESS_STATUS_PROPERTY)]
        public int LastKnownProcessStatus = (int)EProcessStatus.Idle;

        [JsonProperty(LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY)]
        public string LastKnownProcessStatusInfo = "";

        [JsonProperty(MODEL_UNIQUE_NAME_PROPERTY)]
        public string ModelName { get; set; }

        [JsonProperty(MODEL_REVISION_INDEX_PROPERTY)]
        public int RevisionIndex { get; set; }

        [JsonProperty(CURRENT_PROCESS_STAGE_PROPERTY)]
        public int CurrentProcessStage = (int)EProcessStage.Stage0_FileUpload;

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<WorkerVMListDBEntry>(_Content.ToString());

            if (_Content.ContainsKey(VM_NAME_PROPERTY))
                VMName = ContentObject.VMName;
            if (_Content.ContainsKey(VM_STATUS_PROPERTY))
                VMStatus = ContentObject.VMStatus;
            if (_Content.ContainsKey(MODEL_UNIQUE_NAME_PROPERTY))
                ModelName = ContentObject.ModelName;
            if (_Content.ContainsKey(MODEL_REVISION_INDEX_PROPERTY))
                RevisionIndex = ContentObject.RevisionIndex;
            if (_Content.ContainsKey(PROCESS_ID_PROPERTY))
                ProcessId = ContentObject.ProcessId;
            if (_Content.ContainsKey(PROCESS_START_DATE_PROPERTY))
                ProcessStartDate = ContentObject.ProcessStartDate;
            if (_Content.ContainsKey(CURRENT_PROCESS_STAGE_PROPERTY))
                CurrentProcessStage = ContentObject.CurrentProcessStage;
            if (_Content.ContainsKey(LAST_KNOWN_PROCESS_STATUS_INFO_PROPERTY))
                LastKnownProcessStatusInfo = ContentObject.LastKnownProcessStatusInfo;
        }
    }
}
