/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities.Process.RandomAccessFile;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Convert = ServiceUtilities.Process.Convert;
using StreamReader = System.IO.StreamReader;
using ServiceUtilities.Common;

namespace CADFileService.Endpoints.Common
{
    public class CommonMethods
    {
        public static string GetTimeAsCreationTime()
        {
            return Methods.ToISOString();
        }

        public static bool TryParsingRequestFor<T>(
            HttpListenerContext _Context,
            out JObject _SuccessResultJson,
            bool _bDeserialize, out T _SuccessResultDeserialized,
            out BWebServiceResponse _FailureResponse,
            Action<string> _ErrorMessageAction = null)
        {
            _FailureResponse = BWebResponse.InternalError("");
            _SuccessResultDeserialized = default;
            _SuccessResultJson = null;

            string[] MustHaveProperties;
            string[] UpdatableProperties;
            Dictionary<string, Func<JToken, bool>> UpdatablePropertiesValidityCheck;

            if (typeof(T) == typeof(ModelDBEntry))
            {
                MustHaveProperties = ModelDBEntry.MustHaveProperties;
                UpdatableProperties = ModelDBEntry.UpdatableProperties;
                UpdatablePropertiesValidityCheck = ModelDBEntry.UpdatablePropertiesValidityCheck;
            }
            else if (typeof(T) == typeof(Revision))
            {
                MustHaveProperties = Revision.MustHaveProperties;
                UpdatableProperties = Revision.UpdatableProperties;
                UpdatablePropertiesValidityCheck = Revision.UpdatablePropertiesValidityCheck;
            }
            else if (typeof(T) == typeof(FileEntry))
            {
                MustHaveProperties = null;
                UpdatableProperties = FileEntry.UpdatableProperties;
                UpdatablePropertiesValidityCheck = FileEntry.UpdatablePropertiesValidityCheck;
            }
            else throw new ArgumentException("Only ModelDBEntry and Revision are supported.");

            using (var InputStream = _Context.Request.InputStream)
            {
                var NewObjectJson = new JObject();

                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        var PropertiesList = new List<string>();
                        foreach (var Child in ParsedBody)
                        {
                            PropertiesList.Add(Child.Key);
                        }
                        if (MustHaveProperties != null)
                        {
                            foreach (var MustHaveProperty in MustHaveProperties)
                            {
                                if (!PropertiesList.Contains(MustHaveProperty))
                                {
                                    _FailureResponse = BWebResponse.BadRequest("Request body must contain all necessary fields.");
                                    return false;
                                }
                            }
                        }
                        
                        foreach (var Child in ParsedBody)
                        {
                            if (!UpdatablePropertiesValidityCheck[Child.Key](Child.Value))
                            {
                                _FailureResponse = BWebResponse.BadRequest("Given field " + Child.Key + " has invalid value.");
                                return false;
                            }
                            if (UpdatableProperties.Contains(Child.Key))
                            {
                                NewObjectJson[Child.Key] = Child.Value;
                            }
                            else
                            {
                                _FailureResponse = BWebResponse.BadRequest("Unexpected field " + Child.Value.ToString());
                                return false;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        _FailureResponse = BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                        return false;
                    }
                }

                //For other elements to be created without any elements
                _SuccessResultJson = NewObjectJson;
                if (_bDeserialize)
                {
                    _SuccessResultDeserialized = JsonConvert.DeserializeObject<T>(NewObjectJson.ToString());
                }
            }

            return true;
        }

        public static bool TryGettingModelInfo(
            IBDatabaseServiceInterface _DatabaseService,
            string _ModelID,
            out JObject _SuccessResultJson,
            bool _bDeserialize, out ModelDBEntry _SuccessResultDeserialized,
            out BWebServiceResponse _FailureResponse,
            Action<string> _ErrorMessageAction = null)
        {
            _FailureResponse = BWebResponse.InternalError("");
            _SuccessResultDeserialized = null;

            var ModelKey = new BPrimitiveType(_ModelID);

            if (!_DatabaseService.GetItem(
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                ModelKey,
                ModelDBEntry.Properties,
                out _SuccessResultJson,
                _ErrorMessageAction))
            {
                _FailureResponse = BWebResponse.InternalError("Database fetch-model-info operation has failed.");
                return false;
            }
            if (_SuccessResultJson == null)
            {
                _FailureResponse = BWebResponse.NotFound("Model does not exist.");
                return false;
            }

            if (_bDeserialize)
            {
                _SuccessResultDeserialized = JsonConvert.DeserializeObject<ModelDBEntry>(_SuccessResultJson.ToString());
            }

            return true;
        }

        public static bool DoesRevisionExist(ModelDBEntry _Model, int _RevisionIndex, out Revision _SuccessFound, out int _SuccessFoundListIx)
        {
            _SuccessFoundListIx = -1;
            int i = 0;

            foreach (var CurrentRevision in _Model.ModelRevisions)
            {
                if (CurrentRevision.RevisionIndex == _RevisionIndex)
                {
                    _SuccessFound = CurrentRevision;
                    _SuccessFoundListIx = i;
                    return true;
                }
                i++;
            }
            _SuccessFound = null;
            return false;
        }

        public static bool GetProcessedFile(
            WebServiceBaseTimeoutable _Request,
            EProcessedFileType _FileType,
            IBDatabaseServiceInterface _DatabaseService,
            IBFileServiceInterface _FileService,
            string _CadFileStorageBucketName,
            string _ModelUniqueName,
            int _RevisionIndex,
            out BWebServiceResponse _SuccessResponse,
            out BWebServiceResponse _FailureResponse,
            string _GeometryId = null,
            Action<string> _ErrorMessageAction = null)
        {
            var bResult = GetProcessedFile_Internal(
                _Request.InnerProcessor,
                _FileType,
                _DatabaseService,
                _FileService,
                _CadFileStorageBucketName,
                _ModelUniqueName,
                _RevisionIndex,
                out _SuccessResponse,
                out _FailureResponse,
                _GeometryId,
                _ErrorMessageAction);

            return bResult;
        }
        public static bool TryGettingAllInfo(
            IBDatabaseServiceInterface _DatabaseService,
            string _ModelID,
            int _RevisionIndex,
            out ModelDBEntry _ModelObject,
            out Revision _RevisionObject,
            out int _RevisionListIx,
            out BWebServiceResponse _FailureResponse,
            Action<string> _ErrorMessageAction = null)
        {
            _RevisionObject = null;
            _RevisionListIx = -1;
            _FailureResponse = BWebResponse.InternalError("");

            if (!TryGettingModelInfo(
                _DatabaseService,
                _ModelID,
                out JObject _,
                true, out _ModelObject,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                _FailureResponse = FailureResponse;
                return false;
            }

            if (!DoesRevisionExist(
                _ModelObject,
                _RevisionIndex,
                out _RevisionObject,
                out _RevisionListIx))
            {
                _FailureResponse = BWebResponse.NotFound("Revision does not exist.");
                return false;
            }

            return true;
        }

        private static bool GetProcessedFile_Internal(
            WebServiceBaseTimeoutableProcessor _InnerProcessor,
            EProcessedFileType _FileType,
            IBDatabaseServiceInterface _DatabaseService,
            IBFileServiceInterface _FileService,
            string _CadFileStorageBucketName,
            string _ModelUniqueName,
            int _RevisionIndex,
            out BWebServiceResponse _SuccessResponse,
            out BWebServiceResponse _FailureResponse,
            string _GeometryId = null,
            Action<string> _ErrorMessageAction = null)
        {
            _SuccessResponse = BWebResponse.InternalError("");

            if (!TryGettingModelID(
                _DatabaseService,
                _ModelUniqueName,
                out string _ModelID,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                _FailureResponse = FailureResponse;
                return false;
            }

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(_InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _ModelID, _ErrorMessageAction))
            {
                _FailureResponse = BWebResponse.InternalError("Atomic operation control has failed.");
                return false;
            }

            if (!TryGettingAllInfo(
                _DatabaseService,
                _ModelID,
                _RevisionIndex,
                out ModelDBEntry _,
                out Revision RevisionObject,
                out int _,
                out _FailureResponse,
                _ErrorMessageAction))
            {
                return false;
            }

            if (RevisionObject.FileEntry.FileUploadProcessStage != (int)EUploadProcessStage.Uploaded_Processed)
            {
                _FailureResponse = BWebResponse.NotFound("Raw file has not been processed yet.");
                return false;
            }

            string _RelativeFileUrl = RevisionObject.FileEntry.FileRelativeUrl;
            switch (_FileType)
            {
                case EProcessedFileType.UNREAL_HGM:
                    _RelativeFileUrl = RevisionObject.FileEntry.GetFileRelativeUrl(_ModelUniqueName, _RevisionIndex, (int)EProcessStage.Stage6_UnrealEngineConvertion, Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_HGM]);
                    break;
                case EProcessedFileType.UNREAL_HG:
                    _RelativeFileUrl = RevisionObject.FileEntry.GetFileRelativeUrl(_ModelUniqueName, _RevisionIndex, (int)EProcessStage.Stage6_UnrealEngineConvertion, Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_HG]);
                    break;
                case EProcessedFileType.UNREAL_H:
                    _RelativeFileUrl = RevisionObject.FileEntry.GetFileRelativeUrl(_ModelUniqueName, _RevisionIndex, (int)EProcessStage.Stage6_UnrealEngineConvertion, Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_H]);
                    break;
                case EProcessedFileType.UNREAL_G:
                    if (_GeometryId == null)
                    {
                        _ErrorMessageAction?.Invoke("GeometryId was not set when tried to retrieve UnrealGeometry file (u_g)");
                        _FailureResponse = BWebResponse.InternalError("GeometryId was not provided.");
                    }

                    _RelativeFileUrl = RevisionObject.FileEntry.GetFileRelativeUrl(_ModelUniqueName, _RevisionIndex, (int)EProcessStage.Stage6_UnrealEngineConvertion, Constants.ProcessedFileType_Extension_Map[EProcessedFileType.UNREAL_G], _GeometryId);
                    break;
            }

            if (!_FileService.CreateSignedURLForDownload(
                out string DownloadUrl,
                _CadFileStorageBucketName,
                _RelativeFileUrl,
                FileEntry.EXPIRY_MINUTES,
                _ErrorMessageAction))
            {
                _FailureResponse = BWebResponse.InternalError("Signed url generation has failed.");
                return false;
            }

            _SuccessResponse = BWebResponse.StatusOK("File has been located.", new JObject()
            {
                [FileEntry.FILE_DOWNLOAD_URL_PROPERTY] = DownloadUrl,
                [FileEntry.FILE_DOWNLOAD_UPLOAD_EXPIRY_MINUTES_PROPERTY] = FileEntry.EXPIRY_MINUTES
            });

            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(_InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _ModelID, _ErrorMessageAction);

            return true;
        }

        public static bool TryGettingModelID(
            IBDatabaseServiceInterface _DatabaseService, 
            string _RequestedModelName,
            out string _ModelID,
            out BWebServiceResponse _FailureResponse,
            Action<string> _ErrorMessageAction)
        {
            _FailureResponse = BWebResponse.InternalError("");
            _ModelID = string.Empty;
            
            if (!_DatabaseService.GetItem(
                    UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                    UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                    new BPrimitiveType(_RequestedModelName),
                    UniqueFileFieldsDBEntry.Properties,
                    out JObject ModelIDResponse,
                    _ErrorMessageAction) || !ModelIDResponse.ContainsKey(ModelDBEntry.KEY_NAME_MODEL_ID))
            {
                _FailureResponse = BWebResponse.InternalError("Model ID could not be retrieved from the database.");
                return false;
            }

            _ModelID = (string)ModelIDResponse[ModelDBEntry.KEY_NAME_MODEL_ID];
            return true;
        }
    }
}