using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.Endpoints.Utilities;
using CADProcessService.K8S;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.All;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Web;

namespace CADProcessService.Endpoints
{
    public class GetModelProcessTask : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private const string UPLOAD_CONTENT_TYPE = "application/octet-stream";
        private const int UPLOAD_URL_VALIDITY_MINUTES = 1440;

        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBFileServiceInterface FileService;
        private readonly IBMemoryServiceInterface MemoryService;
        private readonly string CadFileStorageBucketName;
        private readonly string BranchName;

        public GetModelProcessTask(
            IBFileServiceInterface _FileService,
            IBDatabaseServiceInterface _DatabaseService,
            IBMemoryServiceInterface _MemoryService,
            string _CadFileStorageBucketName, string _BranchName) : base()
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            MemoryService = _MemoryService;
            BranchName = _BranchName;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }


        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            BWebServiceResponse Response = BWebResponse.NotFound("No task was found to process");

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), "GETTASK", _ErrorMessageAction))
            {
                return BWebResponse.InternalError($"Failed to get access to database record");
            }

            if (DatabaseService.ScanTable(FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), out List<JObject> ConversionItems, _ErrorMessageAction))
            {
                if (ConversionItems != null)
                {
                    foreach (var ConvertItem in ConversionItems)
                    {
                        FileConversionDBEntry Entry = ConvertItem.ToObject<FileConversionDBEntry>();
                        string Key = (string)ConvertItem[FileConversionDBEntry.KEY_NAME_CONVERSION_ID];

                        if (Entry.ConversionStatus == (int)EInternalProcessStage.Queued)
                        {
                            Entry.ConversionStatus = (int)EInternalProcessStage.Processing;

                            if (!DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(Key),
                            JObject.Parse(JsonConvert.SerializeObject(Entry)),
                            out JObject _, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                            {
                                Response = BWebResponse.InternalError("Experienced a Database error");
                            }
                            else
                            {
                                ModelProcessTask Task = new ModelProcessTask();

                                Task.CullingThresholds = Entry.CullingThresholds;
                                Task.GlobalScale = Entry.GlobalScale;
                                Task.GlobalXOffset = Entry.GlobalXOffset;
                                Task.GlobalXRotation = Entry.GlobalXRotation;
                                Task.GlobalYOffset = Entry.GlobalYOffset;
                                Task.GlobalYRotation = Entry.GlobalYRotation;
                                Task.GlobalZOffset = Entry.GlobalZOffset;
                                Task.GlobalZRotation = Entry.GlobalZRotation;
                                Task.LevelThresholds = Entry.LevelThresholds;
                                Task.LodParameters = Entry.LodParameters;
                                Task.ModelName = Entry.ModelName;
                                Task.ModelRevision = Entry.ModelRevision;
                                Task.ProcessStep = Entry.ConversionStage;
                                Task.ConversionId = Key;
                                Task.ZipMainAssemblyFileNameIfAny = Entry.ZipMainAssemblyFileNameIfAny;
                                Task.CustomPythonScript = Entry.CustomPythonScript;

                                Task.Filters = Entry.FilterSettings;

                                FileService.CreateSignedURLForDownload(out string _StageDownloadUrl, Entry.BucketName, $"{BranchName}/{Key}/{Entry.ModelRevision}/stages/{Entry.ConversionStage}/files.zip", 180, _ErrorMessageAction);

                                Task.StageDownloadUrl = _StageDownloadUrl;
                                Response = new BWebServiceResponse(200, new BStringOrStream(JsonConvert.SerializeObject(Task)));
                            }
                        }
                    }
                }
            }
            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), "GETTASK", _ErrorMessageAction);

            return Response;
        }
    }
}
