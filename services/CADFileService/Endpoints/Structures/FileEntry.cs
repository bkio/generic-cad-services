/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using CADFileService.Endpoints.Common;
using ServiceUtilities;
using ServiceUtilities.Common;

namespace CADFileService.Endpoints.Structures
{

    public class FileEntry : IComparable<FileEntry>
    {
        public const string FILE_ENTRY_NAME_PROPERTY = "fileEntryName";
        public const string FILE_ENTRY_FILE_TYPE_PROPERTY = "fileEntryFileType";
        public const string FILE_ENTRY_COMMENTS_PROPERTY = "fileEntryComments";
        public const string FILE_ENTRY_CREATION_TIME_PROPERTY = "fileEntryCreationTime";
        public const string FILE_RELATIVE_URL_PROPERTY = "fileRelativeUrl";
        public const string FILE_PROCESS_STATUS_PROPERTY = "fileProcessStatus";
        public const string FILE_PROCESS_STATUS_INFO_PROPERTY = "fileProcessStatusInfo";
        public const string FILE_PROCESSED_AT_TIME_PROPERTY = "fileProcessedAtTime";
        public const string CURRENT_PROCESS_STAGE_PROPERTY = "currentProcessStage";
        public const string PROCESSED_FILES_ROOT_NODE_ID = "processedFilesRootNodeId";
        public const string ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY = "zipMainAssemblyFileNameIfAny";
        public const string GENERATE_UPLOAD_URL_PROPERTY = "generateUploadUrl";
        public const string DATA_SOURCE_PROPERTY = "dataSource";

        public const string LAYERS_PROPERTY = "layers";
        public const string MERGING_PARTS_PROPERTY = "mergingParts";
        public const string GLOBAL_TRANSFORM_OFFSET_PROPERTY = "globalTransformOffset";
        public const string OPTIMIZATION_PRESET_PROPERTY = "optimizationPreset";
        public const string MERGE_FINAL_LEVEL_PROPERTY = "mergeFinalLevel";
        public const string DELETE_DUPLICATES_PROPERTY = "deleteDuplicates";
        public const string CUSTOM_PYTHON_SCRIPT_PROPERTY = "customPythonScript";

        //Not properties, but being sent in responses
        public const string FILE_DOWNLOAD_URL_PROPERTY = "fileDownloadUrl";
        public const string FILE_UPLOAD_URL_PROPERTY = "fileUploadUrl";
        public const string FILE_UPLOAD_CONTENT_TYPE_PROPERTY = "fileUploadContentType";
        public const string FILE_DOWNLOAD_UPLOAD_EXPIRY_MINUTES_PROPERTY = "expiryMinutes";

        public const int EXPIRY_MINUTES = 30;
        public const string FILE_UPLOAD_CONTENT_TYPE = "application/octet-stream";

        //Update info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            FILE_ENTRY_NAME_PROPERTY,
            FILE_ENTRY_FILE_TYPE_PROPERTY,
            FILE_ENTRY_COMMENTS_PROPERTY,
            ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY,
            GENERATE_UPLOAD_URL_PROPERTY,
            DATA_SOURCE_PROPERTY,
            CURRENT_PROCESS_STAGE_PROPERTY,
            LAYERS_PROPERTY,
            MERGING_PARTS_PROPERTY,
            GLOBAL_TRANSFORM_OFFSET_PROPERTY,
            OPTIMIZATION_PRESET_PROPERTY,
            MERGE_FINAL_LEVEL_PROPERTY,
            DELETE_DUPLICATES_PROPERTY,
            CUSTOM_PYTHON_SCRIPT_PROPERTY
        };

        public static readonly Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck = new Dictionary<string, Func<JToken, bool>>()
        {
            [FILE_ENTRY_NAME_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [FILE_ENTRY_FILE_TYPE_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && SupportedFileFormats.Formats.ContainsKey("." + (((string)_Parameter).ToLower().TrimStart('.')));
            },
            [FILE_ENTRY_COMMENTS_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.String) return false;
                return true;
            },
            [ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [GENERATE_UPLOAD_URL_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.Boolean;
            },
            [DATA_SOURCE_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [CURRENT_PROCESS_STAGE_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.Integer;
            },
            [LAYERS_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JArray)) return false;
                foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.Object) return false;
                return true;
            },
            [MERGING_PARTS_PROPERTY] = (JToken _Parameter) =>
            {
                  if (!(_Parameter is JArray)) return false;
                  foreach (var Cur in _Parameter) if (Cur.Type != JTokenType.Object) return false;
                  return true;
            },
            [GLOBAL_TRANSFORM_OFFSET_PROPERTY] = (JToken _Parameter) =>
            {
                if (!(_Parameter is JObject)) return false;
                return true;
            },
            [OPTIMIZATION_PRESET_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.Integer;
            },
            [MERGE_FINAL_LEVEL_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.Boolean;
            },
            [DELETE_DUPLICATES_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.Boolean;
            },
            [CUSTOM_PYTHON_SCRIPT_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String;
            },
        };

        [JsonProperty(FILE_ENTRY_NAME_PROPERTY)]
        public string FileEntryName = "";
        
        [JsonProperty(FILE_ENTRY_FILE_TYPE_PROPERTY)]
        public string FileEntryFileType = "";

        [JsonProperty(ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY)]
        public string ZipTypeMainAssemblyFileNameIfAny = "";

        [JsonProperty(FILE_ENTRY_COMMENTS_PROPERTY)]
        public List<string> FileEntryComments = new List<string>();

        [JsonProperty(FILE_ENTRY_CREATION_TIME_PROPERTY)]
        public string FileEntryCreationTime = CommonMethods.GetTimeAsCreationTime();

        [JsonProperty(FILE_RELATIVE_URL_PROPERTY)]
        public string FileRelativeUrl = "";

        [JsonProperty(FILE_PROCESS_STATUS_PROPERTY)]
        public int FileProcessStatus = (int)EFileProcessStatus.NotUploaded;
        
        [JsonProperty(FILE_PROCESS_STATUS_INFO_PROPERTY)]
        public string FileProcessStatusInfo = "";

        [JsonProperty(FILE_PROCESSED_AT_TIME_PROPERTY)]
        public string FileProcessedAtTime = "";

        [JsonProperty(PROCESSED_FILES_ROOT_NODE_ID)]
        public ulong ProcessedFilesRootNodeID = 0;

        [JsonProperty(GENERATE_UPLOAD_URL_PROPERTY)]
        public bool bGenerateUploadUrl = false;

        [JsonProperty(DATA_SOURCE_PROPERTY)]
        public string DataSource = "NULL";

        [JsonProperty(CURRENT_PROCESS_STAGE_PROPERTY)]
        public int CurrentProcessStage = (int)EProcessStage.Stage0_FileUpload;

        [JsonProperty(LAYERS_PROPERTY)]
        public List<LayerFilter> Layers = new List<LayerFilter>();
        
        [JsonProperty(MERGING_PARTS_PROPERTY)]
        public List<GenericFilter> MergingParts = new List<GenericFilter>();

        [JsonProperty(GLOBAL_TRANSFORM_OFFSET_PROPERTY)]
        public TransformOffset GlobalTransformOffset = new TransformOffset();

        [JsonProperty(OPTIMIZATION_PRESET_PROPERTY)]
        public int OptimizationPreset = (int)EOptimizationPreset.Default;

        [JsonProperty(MERGE_FINAL_LEVEL_PROPERTY)]
        public bool bMergeFinalLevel = false;
        
        [JsonProperty(DELETE_DUPLICATES_PROPERTY)]
        public bool bDeleteDuplicates = false;

        [JsonProperty(CUSTOM_PYTHON_SCRIPT_PROPERTY)]
        public string CustomPythonScript = "";

        public void Merge(JObject _Content)
        {
            var ContentObject = JsonConvert.DeserializeObject<FileEntry>(_Content.ToString());

            if (_Content.ContainsKey(FILE_ENTRY_NAME_PROPERTY))
                FileEntryName = ContentObject.FileEntryName;
            if (_Content.ContainsKey(FILE_ENTRY_FILE_TYPE_PROPERTY))
                FileEntryFileType = ContentObject.FileEntryFileType;
            if (_Content.ContainsKey(ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY))
                ZipTypeMainAssemblyFileNameIfAny = ContentObject.ZipTypeMainAssemblyFileNameIfAny;
            if (_Content.ContainsKey(FILE_ENTRY_COMMENTS_PROPERTY))
                FileEntryComments = ContentObject.FileEntryComments;
            if (_Content.ContainsKey(FILE_ENTRY_CREATION_TIME_PROPERTY))
                FileEntryCreationTime = ContentObject.FileEntryCreationTime;
            if (_Content.ContainsKey(FILE_RELATIVE_URL_PROPERTY))
                FileRelativeUrl = ContentObject.FileRelativeUrl;
            if (_Content.ContainsKey(FILE_PROCESS_STATUS_PROPERTY))
                FileProcessStatus = ContentObject.FileProcessStatus;
            if (_Content.ContainsKey(FILE_PROCESS_STATUS_INFO_PROPERTY))
                FileProcessStatusInfo = ContentObject.FileProcessStatusInfo;
            if (_Content.ContainsKey(FILE_PROCESSED_AT_TIME_PROPERTY))
                FileProcessedAtTime = ContentObject.FileProcessedAtTime;
            if (_Content.ContainsKey(PROCESSED_FILES_ROOT_NODE_ID))
                ProcessedFilesRootNodeID = ContentObject.ProcessedFilesRootNodeID;
            if (_Content.ContainsKey(GENERATE_UPLOAD_URL_PROPERTY))
                bGenerateUploadUrl = ContentObject.bGenerateUploadUrl;
            if (_Content.ContainsKey(DATA_SOURCE_PROPERTY))
                DataSource = ContentObject.DataSource;

            if (_Content.ContainsKey(CURRENT_PROCESS_STAGE_PROPERTY))
                CurrentProcessStage = ContentObject.CurrentProcessStage;
            if (_Content.ContainsKey(LAYERS_PROPERTY))
                Layers = ContentObject.Layers;
            if (_Content.ContainsKey(MERGING_PARTS_PROPERTY))
                MergingParts = ContentObject.MergingParts;
            if (_Content.ContainsKey(GLOBAL_TRANSFORM_OFFSET_PROPERTY))
                GlobalTransformOffset = ContentObject.GlobalTransformOffset;
            if (_Content.ContainsKey(OPTIMIZATION_PRESET_PROPERTY))
                OptimizationPreset = ContentObject.OptimizationPreset;
            if (_Content.ContainsKey(MERGE_FINAL_LEVEL_PROPERTY))
                bMergeFinalLevel = ContentObject.bMergeFinalLevel;
            if (_Content.ContainsKey(DELETE_DUPLICATES_PROPERTY))
                bDeleteDuplicates = ContentObject.bDeleteDuplicates;
            if (_Content.ContainsKey(CUSTOM_PYTHON_SCRIPT_PROPERTY))
                CustomPythonScript = ContentObject.CustomPythonScript;
        }

        public override bool Equals(object _Other)
        {
            return _Other is FileEntry Casted &&
                   FileRelativeUrl == Casted.FileRelativeUrl;
        }
        public static bool operator ==(FileEntry x, FileEntry y)
        {
            return x.Equals(y);
        }
        public static bool operator !=(FileEntry x, FileEntry y)
        {
            return !x.Equals(y);
        }
        public override int GetHashCode()
        {
            return HashCode.Combine(FileRelativeUrl);
        }
        public int CompareTo(FileEntry _Other)
        {
            return FileRelativeUrl.CompareTo(_Other.FileRelativeUrl);
        }

        public void Prune_NonGettableProperties()
        {
            FileRelativeUrl = "HIDDEN";

            bGenerateUploadUrl = false;
        }

        public void DeleteAllFiles(HttpListenerContext _Context, string _BucketName, Action<string> _ErrorMessageAction = null)
        {
            switch (FileProcessStatus)
            {
                case (int)EFileProcessStatus.ProcessCanceled:
                case (int)EFileProcessStatus.ProcessFailed:
                case (int)EFileProcessStatus.Processed:
                case (int)EFileProcessStatus.Processing:
                    {
                        if(SplitRelativeUrl(
                            FileRelativeUrl, 
                            out string _OwnerModelID,
                            out int _OwnerRevisionIndex,
                            out int _,
                            out bool _,
                            out string _))
                        {
                            for (int CurrentStage = 0; CurrentStage <= (int)EProcessStage.Stage6_UnrealEngineConvertion; CurrentStage++)
                            {
                                var _FileRelativeUrl = GetFileRelativeUrl(_OwnerModelID, _OwnerRevisionIndex, CurrentStage);

                                Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                    _Context,
                                    _BucketName,
                                    _FileRelativeUrl);
                            }
                        }

                        FileRelativeUrl = "";
                        break;
                    }
            }
        }

        public static bool SplitRelativeUrl(
            string _RelativeUrl, 
            out string _OwnerModelID, 
            out int _OwnerRevisionIndex, 
            out int _StageNumber,
            out bool _bIsProcessed,
            out string _RawExtension_IfRaw)
        {
            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();

            _OwnerModelID = null;
            _OwnerRevisionIndex = -1;
            _RawExtension_IfRaw = null;
            _StageNumber = -1;

            _bIsProcessed = false;

            var _Prefix = DeploymentBranchName + "/";
            if (!_RelativeUrl.StartsWith(_Prefix)) return false;
            _RelativeUrl = _RelativeUrl.Substring(_Prefix.Length);

            if (_RelativeUrl == null || _RelativeUrl.Length == 0) return false;
            var Splitted = _RelativeUrl.Split('/');
            if (Splitted.Length < 5) return false;

            _OwnerModelID = Splitted[0];
            if (!int.TryParse(Splitted[1], out _OwnerRevisionIndex)) return false;

            if (!int.TryParse(Splitted[3], out _StageNumber)) return false;

            if (!_bIsProcessed)
            {
                var ExtensionSplit = Splitted[4].Split('.');
                if (ExtensionSplit.Length < 2) return false;
                _RawExtension_IfRaw = ExtensionSplit[ExtensionSplit.Length - 1];
            }

            return true;
        }
        public string GetFileRelativeUrl(string _OwnerModelID, int _OwnerRevisionIndex)
        {
            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();

            if (CurrentProcessStage == (int)EProcessStage.Stage0_FileUpload)
            {
                return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{CurrentProcessStage}/files.{FileEntryFileType}";
            }
            else
            {
                return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{CurrentProcessStage}/files.zip";
            }
        }

        public string GetFileRelativeUrl(string _OwnerModelID, int _OwnerRevisionIndex, int _CurrentProcessStage)
        {
            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();

            if (CurrentProcessStage == (int)EProcessStage.Stage0_FileUpload)
            {
                return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{_CurrentProcessStage}/files.{FileEntryFileType}";
            }
            else
            {
                return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{_CurrentProcessStage}/files.zip";
            }
        }

        public string GetFileRelativeUrl(string _OwnerModelID, int _OwnerRevisionIndex, int _CurrentProcessStage, string _Extension)
        {
            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();

            return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{_CurrentProcessStage}/files.{_Extension}";
        }

        public string GetFileRelativeUrl(string _OwnerModelID, int _OwnerRevisionIndex, int _CurrentProcessStage, string _Extension, string _GeometryId)
        {
            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();

            return $"{DeploymentBranchName}/{_OwnerModelID}/{_OwnerRevisionIndex}/stages/{_CurrentProcessStage}/{_GeometryId}.{_Extension}";
        }
    }
}