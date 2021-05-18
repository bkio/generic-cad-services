using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.Endpoints.Utilities;
using CADProcessService.K8S;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities.All;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace CADProcessService.Endpoints
{
    public class GetModelProcessTask : BppWebServiceBase
    {
        private const string UPLOAD_CONTENT_TYPE = "application/octet-stream";
        private const int UPLOAD_URL_VALIDITY_MINUTES = 1440;

        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBFileServiceInterface FileService;
        private readonly IBMemoryServiceInterface MemoryService;
        private readonly string CadFileStorageBucketName;

        public GetModelProcessTask(
            IBFileServiceInterface _FileService,
            IBDatabaseServiceInterface _DatabaseService,
            IBMemoryServiceInterface _MemoryService,
            string _CadFileStorageBucketName) : base()
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            MemoryService = _MemoryService;
        }

        protected override BWebServiceResponse OnRequestPP(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            BWebServiceResponse Response = BWebResponse.NotFound("No task was found to process");

            MemoryLocker.LockedAction("ProcessTaskCheck", MemoryService, () =>
            {
                if (DatabaseService.ScanTable(FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), out List<JObject> ConversionItems, _ErrorMessageAction))
                {
                    if (ConversionItems != null)
                    {
                        foreach (var ConvertItem in ConversionItems)
                        {
                            FileConversionDBEntry Entry = ConvertItem.ToObject<FileConversionDBEntry>();
                            string Key = (string)ConvertItem[FileConversionDBEntry.KEY_NAME_CONVERSION_ID];
                            int Stage = Entry.ConversionStage;

                            if (Stage < 5 && Entry.ConversionStatus == (int)EInternalProcessStage.Queued)
                            {
                                Entry.ConversionStage = (int)EInternalProcessStage.Processing;

                                if (!DatabaseService.UpdateItem(
                                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                                new BPrimitiveType(Entry.BucketName),
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

                                    FileService.CreateSignedURLForDownload(out string _StageDownloadUrl, Entry.BucketName, $"raw/{Entry.ModelName}/{Entry.ModelRevision}/stages/{Entry.ConversionStage}/Files.zip", 60, _ErrorMessageAction);
                                    Task.StageDownloadUrl = _StageDownloadUrl;

                                    Response = new BWebServiceResponse(200, new BStringOrStream(JsonConvert.SerializeObject(Task)));
                                    return true;
                                }
                            }
                        }
                    }
                }
                return true;
            });

            return Response;
        }
    }
}
