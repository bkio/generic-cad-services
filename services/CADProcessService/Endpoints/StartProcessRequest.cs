/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.IO;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.K8S;
using ServiceUtilities.All;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADProcessService.Endpoints
{
    internal class StartProcessRequest : BppWebServiceBase
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        public StartProcessRequest(IBDatabaseServiceInterface _DatabaseService) : base()
        {
            DatabaseService = _DatabaseService;
        }

        protected override BWebServiceResponse OnRequestPP(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("StartProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            var NewDBEntry = new FileConversionDBEntry()
            {
                ConversionStatus = (int)EInternalProcessStage.Queued
            };

            string NewConversionID_FromRelativeUrl_UrlEncoded = null;
            string BucketName = null;
            string RelativeFileName = null;
            string ZipMainAssembly = "";

            using (var InputStream = _Context.Request.InputStream)
            {
                var NewObjectJson = new JObject();

                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        if (!ParsedBody.ContainsKey("bucketName") ||
                            !ParsedBody.ContainsKey("rawFileRelativeUrl"))
                        {
                            return BWebResponse.BadRequest("Request body must contain all necessary fields.");
                        }
                        var BucketNameToken = ParsedBody["bucketName"];
                        var RawFileRelativeUrlToken = ParsedBody["rawFileRelativeUrl"];

                        if (BucketNameToken.Type != JTokenType.String ||
                            RawFileRelativeUrlToken.Type != JTokenType.String)
                        {
                            return BWebResponse.BadRequest("Request body contains invalid fields.");
                        }

                        if (ParsedBody.ContainsKey("zipTypeMainAssemblyFileNameIfAny"))
                        {
                            var ZipMainAssemblyToken = ParsedBody["zipTypeMainAssemblyFileNameIfAny"];

                            if (ZipMainAssemblyToken.Type != JTokenType.String)
                            {
                                return BWebResponse.BadRequest("Request body contains invalid fields.");
                            }

                            ZipMainAssembly = (string)ZipMainAssemblyToken;
                        }

                        NewDBEntry.BucketName = (string)BucketNameToken;
                        NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode((string)RawFileRelativeUrlToken);

                        BucketName = (string)BucketNameToken;
                        RelativeFileName = (string)RawFileRelativeUrlToken;
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                    }
                }
            }


            if (BucketName == null || RelativeFileName == null)
            {
                return BWebResponse.InternalError("No BucketName or FileName");
            }

            BDatabaseAttributeCondition UpdateCondition = DatabaseService.BuildAttributeNotExistCondition(FileConversionDBEntry.KEY_NAME_CONVERSION_ID);


            //If a process was completed (success or failure) then allow reprocessing
            //Only stop if a process is currently busy processing or already queued
            if (DatabaseService.GetItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                FileConversionDBEntry.Properties,
                out JObject ConversionObject
                ))
            {
                if (ConversionObject != null && ConversionObject.ContainsKey("conversionStatus"))
                {
                    EInternalProcessStage ExistingStatus = (EInternalProcessStage)(int)ConversionObject["conversionStatus"];

                    if (ExistingStatus == EInternalProcessStage.ProcessFailed || ExistingStatus == EInternalProcessStage.ProcessComplete)
                    {
                        UpdateCondition = null;
                    }
                }
            }

            if (!DatabaseService.UpdateItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                UpdateCondition,
                _ErrorMessageAction))
            {
                return BWebResponse.Conflict("File is already being processed/queued.");
            }

            try
            {
                if (BatchProcessingCreationService.Instance.StartBatchProcess(BucketName, RelativeFileName, ZipMainAssembly, out string _PodName, _ErrorMessageAction))
                {
                    //Code for initial method of starting optimizer after pixyz completes
                    //return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
                    if (BatchProcessingCreationService.Instance.StartFileOptimizer(BucketName, RelativeFileName, _ErrorMessageAction))
                    {
                        return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
                    }
                    else
                    {
                        NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                        if (!DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                            JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                            out JObject _, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                        {
                            return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                        }

                        //Try kill pixyz pod that we have succeeded in creating
                        if (!BatchProcessingCreationService.Instance.TryKillPod(_PodName, "cip-batch"))
                        {
                            return BWebResponse.InternalError("Failed to start the unreal optimizer and failed to kill pixyz pod");
                        }

                        return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                    }

                }
                else
                {
                    NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                    if (!DatabaseService.UpdateItem(
                        FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                        FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                        new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                        JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                        out JObject _, EBReturnItemBehaviour.DoNotReturn,
                        null,
                        _ErrorMessageAction))
                    {
                        return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                    }

                    return BWebResponse.InternalError("Failed to start the batch process");
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");

                NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                if (!DatabaseService.UpdateItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                    JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                    out JObject _, EBReturnItemBehaviour.DoNotReturn,
                    null,
                    _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                }

                return BWebResponse.InternalError("Failed to start the batch process");
            }
        }
    }
}