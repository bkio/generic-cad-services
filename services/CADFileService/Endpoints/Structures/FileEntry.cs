/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using CADFileService.Endpoints.Common;

namespace CADFileService.Endpoints.Structures
{
    public class FileEntry : IComparable<FileEntry>
    {
        public const string FILE_ENTRY_NAME_PROPERTY = "fileEntryName";
        public const string FILE_ENTRY_FILE_TYPE_PROPERTY = "fileEntryFileType";
        public const string FILE_ENTRY_COMMENTS_PROPERTY = "fileEntryComments";
        public const string FILE_ENTRY_CREATION_TIME_PROPERTY = "fileEntryCreationTime";
        public const string RAW_FILE_RELATIVE_URL_PROPERTY = "rawFileRelativeUrl";
        public const string FILE_PROCESS_STAGE_PROPERTY = "fileProcessStage";
        public const string FILE_PROCESSED_AT_TIME_PROPERTY = "fileProcessedAtTime";
        public const string PROCESSED_FILES_ROOT_NODE_ID = "processedFilesRootNodeId";
        public const string HIERARCHY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY = "hierarchyRAFRelativeUrl";
        public const string HIERARCHY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY = "hierarchyCFRelativeUrl";
        public const string GEOMETRY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY = "geometryRAFRelativeUrl";
        public const string GEOMETRY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY = "geometryCFRelativeUrl";
        public const string METADATA_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY = "metadataRAFRelativeUrl";
        public const string METADATA_COMPRESSED_FILE_RELATIVE_URL_PROPERTY = "metadataCFRelativeUrl";
        public const string ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY = "zipMainAssemblyFileNameIfAny";
        public const string GENERATE_UPLOAD_URL_PROPERTY = "generateUploadUrl";
        public const string DATA_SOURCE_PROPERTY = "dataSource";
        public const string UNREAL_HGM_FILE_RELATIVE_URL_PROPERTY = "unrealHGMRelativeUrl";
        public const string UNREAL_HG_FILE_RELATIVE_URL_PROPERTY = "unrealHGRelativeUrl";
        public const string UNREAL_H_FILE_RELATIVE_URL_PROPERTY = "unrealHRelativeUrl";
        public const string UNREAL_G_FILES_RELATIVE_URL_PROPERTY = "unrealGRelativeUrlBasePath";

        //Not properties, but being sent in responses
        public const string FILE_DOWNLOAD_URL_PROPERTY = "fileDownloadUrl";
        public const string FILE_UPLOAD_URL_PROPERTY = "fileUploadUrl";
        public const string FILE_UPLOAD_CONTENT_TYPE_PROPERTY = "fileUploadContentType";
        public const string FILE_DOWNLOAD_UPLOAD_EXPIRY_MINUTES_PROPERTY = "expiryMinutes";

        public const int EXPIRY_MINUTES = 5;
        public const string RAW_FILE_UPLOAD_CONTENT_TYPE = "application/octet-stream";

        //Update info call can change these fields.
        public static readonly string[] UpdatableProperties =
        {
            FILE_ENTRY_NAME_PROPERTY,
            FILE_ENTRY_FILE_TYPE_PROPERTY,
            FILE_ENTRY_COMMENTS_PROPERTY,
            ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY,
            GENERATE_UPLOAD_URL_PROPERTY,
            DATA_SOURCE_PROPERTY
        };

        public static readonly Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck = new Dictionary<string, Func<JToken, bool>>()
        {
            [FILE_ENTRY_NAME_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && ((string)_Parameter).Length > 0;
            },
            [FILE_ENTRY_FILE_TYPE_PROPERTY] = (JToken _Parameter) =>
            {
                return _Parameter.Type == JTokenType.String && CADFileService.Endpoints.Common.SupportedFileFormats.Formats.ContainsKey("." + (((string)_Parameter).ToLower().TrimStart('.')));
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
            }
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

        [JsonProperty(RAW_FILE_RELATIVE_URL_PROPERTY)]
        public string RawFileRelativeUrl = "";

        [JsonProperty(FILE_PROCESS_STAGE_PROPERTY)]
        public int FileProcessStage = (int)Constants.EProcessStage.NotUploaded;

        [JsonProperty(FILE_PROCESSED_AT_TIME_PROPERTY)]
        public string FileProcessedAtTime = "";

        [JsonProperty(PROCESSED_FILES_ROOT_NODE_ID)]
        public ulong ProcessedFilesRootNodeID = 0;

        [JsonProperty(HIERARCHY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY)]
        public string HierarchyRAFRelativeUrl = "";

        [JsonProperty(HIERARCHY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY)]
        public string HierarchyCFRelativeUrl = "";

        [JsonProperty(GEOMETRY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY)]
        public string GeometryRAFRelativeUrl = "";

        [JsonProperty(GEOMETRY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY)]
        public string GeometryCFRelativeUrl = "";

        [JsonProperty(METADATA_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY)]
        public string MetadataRAFRelativeUrl = "";

        [JsonProperty(METADATA_COMPRESSED_FILE_RELATIVE_URL_PROPERTY)]
        public string MetadataCFRelativeUrl = "";

        [JsonProperty(GENERATE_UPLOAD_URL_PROPERTY)]
        public bool bGenerateUploadUrl = false;

        [JsonProperty(DATA_SOURCE_PROPERTY)]
        public string DataSource = "NULL";

        [JsonProperty(UNREAL_HGM_FILE_RELATIVE_URL_PROPERTY)]
        public string UnrealHGMRelativeUrl = "";
        [JsonProperty(UNREAL_HG_FILE_RELATIVE_URL_PROPERTY)]
        public string UnrealHGRelativeUrl = "";
        [JsonProperty(UNREAL_H_FILE_RELATIVE_URL_PROPERTY)]
        public string UnrealHRelativeUrl = "";
        [JsonProperty(UNREAL_G_FILES_RELATIVE_URL_PROPERTY)]
        public string UnrealGRelativeUrlBasePath = "";

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
            if (_Content.ContainsKey(RAW_FILE_RELATIVE_URL_PROPERTY))
                RawFileRelativeUrl = ContentObject.RawFileRelativeUrl;
            if (_Content.ContainsKey(FILE_PROCESS_STAGE_PROPERTY))
                FileProcessStage = ContentObject.FileProcessStage;
            if (_Content.ContainsKey(FILE_PROCESSED_AT_TIME_PROPERTY))
                FileProcessedAtTime = ContentObject.FileProcessedAtTime;
            if (_Content.ContainsKey(PROCESSED_FILES_ROOT_NODE_ID))
                ProcessedFilesRootNodeID = ContentObject.ProcessedFilesRootNodeID;
            if (_Content.ContainsKey(HIERARCHY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY))
                HierarchyRAFRelativeUrl = ContentObject.HierarchyRAFRelativeUrl;
            if (_Content.ContainsKey(HIERARCHY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY))
                HierarchyCFRelativeUrl = ContentObject.HierarchyCFRelativeUrl;
            if (_Content.ContainsKey(GEOMETRY_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY))
                GeometryRAFRelativeUrl = ContentObject.GeometryRAFRelativeUrl;
            if (_Content.ContainsKey(GEOMETRY_COMPRESSED_FILE_RELATIVE_URL_PROPERTY))
                GeometryCFRelativeUrl = ContentObject.GeometryCFRelativeUrl;
            if (_Content.ContainsKey(METADATA_RANDOM_ACCESSIBLE_FILE_RELATIVE_URL_PROPERTY))
                MetadataRAFRelativeUrl = ContentObject.MetadataRAFRelativeUrl;
            if (_Content.ContainsKey(METADATA_COMPRESSED_FILE_RELATIVE_URL_PROPERTY))
                MetadataCFRelativeUrl = ContentObject.MetadataCFRelativeUrl;

            if (_Content.ContainsKey(UNREAL_HGM_FILE_RELATIVE_URL_PROPERTY))
                UnrealHGMRelativeUrl = ContentObject.UnrealHGMRelativeUrl;
            if (_Content.ContainsKey(UNREAL_HG_FILE_RELATIVE_URL_PROPERTY))
                UnrealHGRelativeUrl = ContentObject.UnrealHGRelativeUrl;
            if (_Content.ContainsKey(UNREAL_H_FILE_RELATIVE_URL_PROPERTY))
                UnrealHRelativeUrl = ContentObject.UnrealHRelativeUrl;
            if (_Content.ContainsKey(UNREAL_G_FILES_RELATIVE_URL_PROPERTY))
                UnrealGRelativeUrlBasePath = ContentObject.UnrealGRelativeUrlBasePath;

            if (_Content.ContainsKey(GENERATE_UPLOAD_URL_PROPERTY))
                bGenerateUploadUrl = ContentObject.bGenerateUploadUrl;
            if (_Content.ContainsKey(DATA_SOURCE_PROPERTY))
                DataSource = ContentObject.DataSource;
        }

        public override bool Equals(object _Other)
        {
            return _Other is FileEntry Casted &&
                   RawFileRelativeUrl == Casted.RawFileRelativeUrl;
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
            return HashCode.Combine(RawFileRelativeUrl);
        }
        public int CompareTo(FileEntry _Other)
        {
            return RawFileRelativeUrl.CompareTo(_Other.RawFileRelativeUrl);
        }

        public void Prune_NonGettableProperties()
        {
            RawFileRelativeUrl = "HIDDEN";

            HierarchyRAFRelativeUrl = "HIDDEN";
            HierarchyCFRelativeUrl = "HIDDEN";
            GeometryRAFRelativeUrl = "HIDDEN";
            GeometryCFRelativeUrl = "HIDDEN";
            MetadataRAFRelativeUrl = "HIDDEN";
            MetadataCFRelativeUrl = "HIDDEN";

            UnrealHGMRelativeUrl = "HIDDEN";
            UnrealHGRelativeUrl = "HIDDEN";
            UnrealHRelativeUrl = "HIDDEN";
            UnrealGRelativeUrlBasePath = "HIDDEN";

            bGenerateUploadUrl = false;
        }

        public void DeleteAllFiles(HttpListenerContext _Context, string _BucketName, Action<string> _ErrorMessageAction = null)
        {
            switch (FileProcessStage)
            {
                case (int)Constants.EProcessStage.Uploaded_ProcessFailed:
                case (int)Constants.EProcessStage.Uploaded_Processed:
                case (int)Constants.EProcessStage.Uploaded_Processing:
                    {
                        Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                            _Context,
                            _BucketName,
                            RawFileRelativeUrl);

                        RawFileRelativeUrl = "";

                        if (FileProcessStage == (int)Constants.EProcessStage.Uploaded_Processed)
                        {
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                HierarchyRAFRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                HierarchyCFRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                GeometryRAFRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                GeometryCFRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                MetadataRAFRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                MetadataCFRelativeUrl);

                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                UnrealHGMRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                UnrealHGRelativeUrl);
                            Controller_DeliveryEnsurer.Get().FS_DeleteFile_FireAndForget(
                                _Context,
                                _BucketName,
                                UnrealHRelativeUrl);

                            Controller_DeliveryEnsurer.Get().FS_DeleteFolder_FireAndForget(
                                _Context,
                                _BucketName,
                                UnrealGRelativeUrlBasePath);

                            HierarchyRAFRelativeUrl = "";
                            HierarchyCFRelativeUrl = "";
                            GeometryRAFRelativeUrl = "";
                            GeometryCFRelativeUrl = "";
                            MetadataRAFRelativeUrl = "";
                            MetadataCFRelativeUrl = "";

                            UnrealHGMRelativeUrl = "";
                            UnrealHGRelativeUrl = "";
                            UnrealHRelativeUrl = "";
                            UnrealGRelativeUrlBasePath = "";
                        }
                        break;
                    }
            }
        }

        public const string RAW_FILE_FOLDER_PREFIX = "raw/";

        public static bool SplitRelativeUrl(
            string _RelativeUrl, 
            out string _OwnerModelID, 
            out int _OwnerRevisionIndex, 
            out bool _bIsProcessed, 
            out EProcessedFileType _ProcessedFileType_IfProcessed, 
            out string _RawExtension_IfRaw)
        {
            _OwnerModelID = null;
            _OwnerRevisionIndex = -1;
            _ProcessedFileType_IfProcessed = EProcessedFileType.NONE_OR_RAW;
            _RawExtension_IfRaw = null;

            _bIsProcessed = false;
            foreach (var FolderPrefix in Constants.ProcessedFileType_FolderPrefix_Map)
            {
                if (_RelativeUrl.StartsWith(FolderPrefix.Value))
                {
                    _ProcessedFileType_IfProcessed = FolderPrefix.Key;
                    _bIsProcessed = true;
                    _RelativeUrl = _RelativeUrl.Substring(FolderPrefix.Value.Length);
                }
            }
            if (_ProcessedFileType_IfProcessed == EProcessedFileType.NONE_OR_RAW)
            {
                if (!_RelativeUrl.StartsWith(RAW_FILE_FOLDER_PREFIX)) return false;
                _RelativeUrl = _RelativeUrl.Substring(RAW_FILE_FOLDER_PREFIX.Length);
            }

            if (_RelativeUrl == null || _RelativeUrl.Length == 0) return false;
            var Splitted = _RelativeUrl.Split('/');
            if (Splitted.Length < 3) return false;

            _OwnerModelID = Splitted[0];
            if (!int.TryParse(Splitted[1], out _OwnerRevisionIndex)) return false;
            
            if (!_bIsProcessed)
            {
                var ExtensionSplit = Splitted[2].Split('.');
                if (ExtensionSplit.Length < 2) return false;
                _RawExtension_IfRaw = ExtensionSplit[ExtensionSplit.Length - 1];
            }

            return true;
        }
        public string SetRelativeUrls_GetCommonUrlPart_FileEntryFileTypePreSet(string _OwnerModelID, int _OwnerRevisionIndex)
        {
            var CommonUrlPart = $"{_OwnerModelID}/{_OwnerRevisionIndex}/file.";

            RawFileRelativeUrl = Make_RawFileRelativeUrl_FromCommonUrlPart(CommonUrlPart, FileEntryFileType);

            HierarchyRAFRelativeUrl = Make_HierarchyRAFRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            HierarchyCFRelativeUrl = Make_HierarchyCFRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            GeometryRAFRelativeUrl = Make_GeometryRAFRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            GeometryCFRelativeUrl = Make_GeometryCFRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            MetadataRAFRelativeUrl = Make_MetadataRAFRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            MetadataCFRelativeUrl = Make_MetadataCFRelativeUrl_FromCommonUrlPart(CommonUrlPart);

            UnrealHGMRelativeUrl = Make_UnrealHGMRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            UnrealHGRelativeUrl = Make_UnrealHGRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            UnrealHRelativeUrl = Make_UnrealHRelativeUrl_FromCommonUrlPart(CommonUrlPart);
            UnrealGRelativeUrlBasePath = Make_UnrealGCFRelativeUrl_FromCommonUrlPart($"{_OwnerModelID}/{_OwnerRevisionIndex}/");

            return CommonUrlPart;
        }
        public static string Make_RawFileRelativeUrl_FromCommonUrlPart(string _CommonUrlPart, string _FileEntryFileType)
        {
            return RAW_FILE_FOLDER_PREFIX + _CommonUrlPart + _FileEntryFileType.ToLower();
        }
        public static string Make_HierarchyRAFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_RAF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.HIERARCHY_RAF];
        }
        public static string Make_HierarchyCFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_CF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.HIERARCHY_CF];
        }
        public static string Make_GeometryRAFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_RAF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.GEOMETRY_RAF];
        }
        public static string Make_GeometryCFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_CF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.GEOMETRY_CF];
        }
        public static string Make_MetadataRAFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_RAF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.METADATA_RAF];
        }
        public static string Make_MetadataCFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_CF] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.METADATA_CF];
        }

        public static string Make_UnrealHGMRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_HGM] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_HGM];
        }
        public static string Make_UnrealHGRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_HG] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_HG];
        }
        public static string Make_UnrealHRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_H] + _CommonUrlPart + Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_H];
        }
        public static string Make_UnrealGCFRelativeUrl_FromCommonUrlPart(string _CommonUrlPart)
        {
            return Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_G] + _CommonUrlPart;
        }
    }
}