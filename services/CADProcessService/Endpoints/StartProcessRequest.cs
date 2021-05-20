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
using System.Collections.Generic;
using ServiceUtilities_All.Common;
using CADProcessService.Endpoints.Utilities;

namespace CADProcessService.Endpoints
{
    internal class StartProcessRequest : BppWebServiceBase
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBVMServiceInterface VMService;
        private readonly IBMemoryServiceInterface MemoryService;
        public Dictionary<string, string> VirtualMachines = new Dictionary<string, string>();

        public StartProcessRequest(IBMemoryServiceInterface _MemoryService, IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VMService, Dictionary<string, string> _VirtualMachines) : base()
        {
            MemoryService = _MemoryService;
            DatabaseService = _DatabaseService;
            VMService = _VMService;
            VirtualMachines = _VirtualMachines;
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

                        if(ParsedBody.ContainsKey("modelName"))
                        {
                            NewDBEntry.ModelName = (string)ParsedBody["modelName"];
                        }
                        
                        if (ParsedBody.ContainsKey("modelRevision"))
                        {
                            NewDBEntry.ModelRevision = (int)ParsedBody["modelRevision"];
                        }

                        if (ParsedBody.ContainsKey("processStep"))
                        {
                            NewDBEntry.ConversionStage = (int)ParsedBody["processStep"];
                        }

                        if (ParsedBody.ContainsKey("globalScale"))
                        {
                            NewDBEntry.GlobalScale = (float)ParsedBody["globalScale"];
                        }

                        if (ParsedBody.ContainsKey("globalXOffset"))
                        {
                            NewDBEntry.GlobalXOffset = (float)ParsedBody["globalXOffset"];
                        }

                        if (ParsedBody.ContainsKey("globalYOffset"))
                        {
                            NewDBEntry.GlobalYOffset = (float)ParsedBody["globalYOffset"];
                        }

                        if (ParsedBody.ContainsKey("globalZOffset"))
                        {
                            NewDBEntry.GlobalZOffset = (float)ParsedBody["globalZOffset"];
                        }

                        if (ParsedBody.ContainsKey("globalXRotation"))
                        {
                            NewDBEntry.GlobalXRotation = (float)ParsedBody["globalXRotation"];
                        }

                        if (ParsedBody.ContainsKey("globalYRotation"))
                        {
                            NewDBEntry.GlobalYRotation = (float)ParsedBody["globalYRotation"];
                        }

                        if (ParsedBody.ContainsKey("globalZRotation"))
                        {
                            NewDBEntry.GlobalZRotation = (float)ParsedBody["globalZRotation"];
                        }

                        if (ParsedBody.ContainsKey("levelThresholds"))
                        {
                            NewDBEntry.LevelThresholds = ParsedBody["levelThresholds"].ToObject<float[]>();
                        }

                        if (ParsedBody.ContainsKey("lodParameters"))
                        {
                            NewDBEntry.LodParameters = (string)ParsedBody["lodParameters"];
                        }
                        if (ParsedBody.ContainsKey("cullingThresholds"))
                        {
                            NewDBEntry.CullingThresholds = (string)ParsedBody["cullingThresholds"];
                        }
                        NewDBEntry.QueuedTime = DateTime.UtcNow.ToString();
                        
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


            //Lock to avoid race condition with deallocation
            MemoryLocker.LockedAction("UpdateVmStatus", MemoryService, () =>
            {
                WorkerVMListDBEntry VmEntry = GetAvailableVm(out string _VMID, out string _VMName, _ErrorMessageAction);
                VmEntry.CurrentProcessStage = NewDBEntry.ConversionStage;

                VmEntry.StageProcessStartDates.Clear();
                VmEntry.StageProcessStartDates.Add(DateTime.Now);
                VmEntry.ProcessStartDate = DateTime.Now;

                StartVM(_VMName, VmEntry, () =>
                {

                    VmEntry.VMStatus = (int)EVMStatus.Busy;

                    DatabaseService.UpdateItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(_VMID),
                    JObject.Parse(JsonConvert.SerializeObject(VmEntry)),
                    out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                    UpdateCondition,
                    _ErrorMessageAction);

                }, _ErrorMessageAction);

                return true;
            });

            return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");

            //try
            //{
            //    if (BatchProcessingCreationService.Instance.StartBatchProcess(BucketName, RelativeFileName, ZipMainAssembly, out string _PodName, _ErrorMessageAction))
            //    {
            //        //Code for initial method of starting optimizer after pixyz completes
            //        //return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
            //        if (BatchProcessingCreationService.Instance.StartFileOptimizer(BucketName, RelativeFileName, _ErrorMessageAction))
            //        {
            //            return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
            //        }
            //        else
            //        {
            //            NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

            //            if (!DatabaseService.UpdateItem(
            //                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
            //                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
            //                new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
            //                JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
            //                out JObject _, EBReturnItemBehaviour.DoNotReturn,
            //                null,
            //                _ErrorMessageAction))
            //            {
            //                return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
            //            }

            //            //Try kill pixyz pod that we have succeeded in creating
            //            if (!BatchProcessingCreationService.Instance.TryKillPod(_PodName, "cip-batch"))
            //            {
            //                return BWebResponse.InternalError("Failed to start the unreal optimizer and failed to kill pixyz pod");
            //            }

            //            return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
            //        }
            //    }
            //    else
            //    {
            //        NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

            //        if (!DatabaseService.UpdateItem(
            //            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
            //            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
            //            new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
            //            JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
            //            out JObject _, EBReturnItemBehaviour.DoNotReturn,
            //            null,
            //            _ErrorMessageAction))
            //        {
            //            return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
            //        }

            //        return BWebResponse.InternalError("Failed to start the batch process");
            //    }
            //}
            //catch (Exception ex)
            //{
            //    _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");

            //    NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

            //    if (!DatabaseService.UpdateItem(
            //        FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
            //        FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
            //        new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
            //        JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
            //        out JObject _, EBReturnItemBehaviour.DoNotReturn,
            //        null,
            //        _ErrorMessageAction))
            //    {
            //        return BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
            //    }

            //    return BWebResponse.InternalError("Failed to start the batch process");
            //}
        }

        public WorkerVMListDBEntry GetAvailableVm(out string _Id, out string _VmName, Action<string> _ErrorMessageAction)
        {
            //Fetch VM entries
            //Find VM that is available and return name.
            foreach(var vm in VirtualMachines)
            {
                if(DatabaseService.GetItem(
                WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                new BPrimitiveType(vm.Key),
                FileConversionDBEntry.Properties,
                out JObject VMEntry
                ))
                {
                    WorkerVMListDBEntry CurrentEntry = VMEntry.ToObject<WorkerVMListDBEntry>();

                    if ((EVMStatus)CurrentEntry.VMStatus == EVMStatus.Available)
                    {
                        _Id = vm.Key;
                        _VmName = vm.Value;
                        return CurrentEntry;
                    }
                }
            }
            _Id = null;
            _VmName = null;
            return null;
        }

        private void StartVM(string _VMName, WorkerVMListDBEntry _vm, Action VMStartFailureAction, Action<string> _ErrorMessageAction)
        {
            if (_vm != null)
            {
                VMService.StartInstances(new string[] { }, () =>
                {

                },
                () =>
                {
                    _ErrorMessageAction?.Invoke($"FAILED TO START VM [{_VMName}]");
                    VMStartFailureAction();
                },
                    _ErrorMessageAction
                );
            }else
            {
                _ErrorMessageAction?.Invoke($"NO AVAILABLE VM WAS FOUND[{_VMName}]");
                VMStartFailureAction();
            }
        }
    }
}