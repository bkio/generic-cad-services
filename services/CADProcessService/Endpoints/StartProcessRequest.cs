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
using CADProcessService.Endpoints.Utilities;
using ServiceUtilities.Common;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using ServiceUtilities;
using System.Threading;

namespace CADProcessService.Endpoints
{
    internal class StartProcessRequest : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBVMServiceInterface VirtualMachineService;
        private readonly Dictionary<string, string> VirtualMachineDictionary;
        private readonly string CadProcessServiceUrl;
        private readonly Dictionary<int, OptimizationPresetEntry> Presets = new Dictionary<int, OptimizationPresetEntry>();

        public StartProcessRequest(IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VirtualMachineService, Dictionary<string, string> _VirtualMachineDictionary, string _CadProcessServiceUrl) : base()
        {
            CreateDefaultPresets();
            DatabaseService = _DatabaseService;
            VirtualMachineService = _VirtualMachineService;
            VirtualMachineDictionary = _VirtualMachineDictionary;
            CadProcessServiceUrl = _CadProcessServiceUrl;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            //TestVMStart(_ErrorMessageAction);
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
            //string ZipMainAssembly = "";

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

                        if (ParsedBody.ContainsKey("modelName"))
                        {
                            NewDBEntry.ModelName = (string)ParsedBody["modelName"];
                        }

                        if (ParsedBody.ContainsKey("modelRevision"))
                        {
                            NewDBEntry.ModelRevision = (int)ParsedBody["modelRevision"];
                        }

                        if (ParsedBody.ContainsKey("zipMainAssemblyFileNameIfAny"))
                        {
                            NewDBEntry.ZipMainAssemblyFileNameIfAny = (string)ParsedBody["zipMainAssemblyFileNameIfAny"];
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
                            NewDBEntry.LevelThresholds = (string)ParsedBody["levelThresholds"];
                        }

                        if (ParsedBody.ContainsKey("lodParameters"))
                        {
                            NewDBEntry.LodParameters = (string)ParsedBody["lodParameters"];
                        }
                        if (ParsedBody.ContainsKey("cullingThresholds"))
                        {
                            NewDBEntry.CullingThresholds = (string)ParsedBody["cullingThresholds"];
                        }
                        if (ParsedBody.ContainsKey("filters"))
                        {
                            NewDBEntry.FilterSettings = (string)ParsedBody["filters"];
                        }
                        if (ParsedBody.ContainsKey("deleteDuplicates"))
                        {
                            NewDBEntry.DeleteDuplicates = (string)ParsedBody["deleteDuplicates"];
                        }

                        if (ParsedBody.ContainsKey("mergeFinalLevel"))
                        {
                            NewDBEntry.MergeFinalLevel = (string)ParsedBody["mergeFinalLevel"];
                        }

                        //If Preset is used then override optimization parameters
                        if (ParsedBody.ContainsKey("optimizationPreset"))
                        {
                            int presetId = (int)ParsedBody["optimizationPreset"];

                            OptimizationPresetEntry Preset = Presets[presetId];
                            NewDBEntry.LodParameters = Preset.LodParameters;
                            NewDBEntry.CullingThresholds = Preset.CullingThresholds;
                            NewDBEntry.LevelThresholds = Preset.DistanceThresholds;
                        }


                        NewDBEntry.QueuedTime = DateTime.UtcNow.ToString();

                        //if (ParsedBody.ContainsKey("zipTypeMainAssemblyFileNameIfAny"))
                        //{
                        //    var ZipMainAssemblyToken = ParsedBody["zipTypeMainAssemblyFileNameIfAny"];

                        //    if (ZipMainAssemblyToken.Type != JTokenType.String)
                        //    {
                        //        return BWebResponse.BadRequest("Request body contains invalid fields.");
                        //    }

                        //    ZipMainAssembly = (string)ZipMainAssemblyToken;
                        //}

                        NewDBEntry.BucketName = (string)BucketNameToken;
                        //NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode((string)RawFileRelativeUrlToken);
                        NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode($"raw/{NewDBEntry.ModelName}/{NewDBEntry.ModelRevision}");

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
                return BWebResponse.InternalError("StartProcessRequest: No BucketName or FileName");
            }

            //BDatabaseAttributeCondition UpdateCondition = DatabaseService.BuildAttributeNotExistCondition(FileConversionDBEntry.KEY_NAME_CONVERSION_ID);

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), NewConversionID_FromRelativeUrl_UrlEncoded, _ErrorMessageAction))
            {
                return BWebResponse.InternalError($"StartProcessRequest: Failed to get access to database record");
            }

            try
            {
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

                        //if (ExistingStatus == EInternalProcessStage.ProcessFailed || ExistingStatus == EInternalProcessStage.ProcessComplete)
                        //{
                        //    UpdateCondition = null;

                        //}
                        if (ExistingStatus == EInternalProcessStage.Processing)
                        {
                            return BWebResponse.Conflict("StartProcessRequest: File is already being processed/queued.");
                        }

                    }
                }

                if (!DatabaseService.UpdateItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                    JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                    out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                    null,
                    _ErrorMessageAction))
                {
                    return BWebResponse.Conflict("StartProcessRequest: File is already being processed/queued.");
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return BWebResponse.InternalError($"StartProcessRequest->DBSERVICE_FILE_CONVERSIONS_TABLE: UpdateItem Database Error");
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), NewConversionID_FromRelativeUrl_UrlEncoded, _ErrorMessageAction);
            }



            //Lock to avoid race condition with deallocation
            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), "VMLOCK", _ErrorMessageAction))
            {
                return BWebResponse.InternalError($"StartProcessRequest->DBSERVICE_WORKERS_VM_LIST_TABLE: Failed to get access to database lock");
            }

            try
            {
                WorkerVMListDBEntry VmEntry = GetAvailableVm(out string _VMID, out string _VMName, _ErrorMessageAction);
                VmEntry.CurrentProcessStage = NewDBEntry.ConversionStage;
                VmEntry.LastKnownProcessStatus = NewDBEntry.ConversionStage;
                VmEntry.ProcessStartDate = DateTime.Now.ToString();
                VmEntry.VMStatus = (int)EVMStatus.Busy;
                VmEntry.ModelName = NewDBEntry.ModelName;
                VmEntry.ProcessId = NewConversionID_FromRelativeUrl_UrlEncoded;
                VmEntry.RevisionIndex = NewDBEntry.ModelRevision;

                StartVirtualMachine(_VMID, _VMName, VmEntry, () =>
                {
                    DatabaseService.UpdateItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(_VMID),
                    JObject.Parse(JsonConvert.SerializeObject(VmEntry)),
                    out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                    null,
                    _ErrorMessageAction);

                }, _ErrorMessageAction);
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return BWebResponse.InternalError($"StartProcessRequest->GetAvailableVm: Database Error");
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), "VMLOCK", _ErrorMessageAction);
            }

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
            foreach (var vm in VirtualMachineDictionary)
            {
                if (DatabaseService.GetItem(
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

        private void StartVirtualMachine(string _VirtualMachineId, string _VirtualMachineName, WorkerVMListDBEntry _VirtualMachineEntry, System.Action VMStartFailureAction, Action<string> _ErrorMessageAction)
        {
            if (_VirtualMachineEntry != null)
            {
                VirtualMachineService.StartInstances(new string[] { _VirtualMachineName }, () =>
                {
                    _ErrorMessageAction?.Invoke($"Virtual Machine has been started. Name: [{_VirtualMachineName}]");
                    VirtualMachineService.RunCommand(new string[] { _VirtualMachineName }, EBVMOSType.Windows,
                            new string[] {
                                $"Start-Process \"cmd.exe\" \"/c C:\\Applet\\LaunchUpdater.bat {_VirtualMachineId} {CadProcessServiceUrl}\"",
                            },
                            () => {
                                _ErrorMessageAction?.Invoke($"Command has been executed. Name: [{_VirtualMachineName}]");
                            },
                            () => {
                                _ErrorMessageAction?.Invoke($"Command execution has been failed. Name: [{_VirtualMachineName}]");
                            }
                        );
                },
                () =>
                {
                    _ErrorMessageAction?.Invoke($"Virtual Machine starting has been failed. Name: [{_VirtualMachineName}]");
                    VMStartFailureAction();
                },
                    _ErrorMessageAction
                );
            }
            else
            {
                _ErrorMessageAction?.Invoke($"No available virtual machine was found. Name: [{_VirtualMachineName}]");
                VMStartFailureAction();
            }
        }

        //private void TestVMStart(Action<string> _ErrorMessageAction)
        //{
        //    ManualResetEvent Wait = new ManualResetEvent(false);
        //    string _VirtualMachineName = "cip-vm-development-0";
        //    if (_VirtualMachineName != null)
        //    {
        //        VirtualMachineService.StartInstances(new string[] { _VirtualMachineName }, () =>
        //        {
        //            _ErrorMessageAction?.Invoke($"Virtual Machine has been started. Name: [{_VirtualMachineName}]");
        //            VirtualMachineService.RunCommand(new string[] { _VirtualMachineName }, EBVMOSType.Windows,
        //                    new string[] {
        //                        $"Start-Process \"cmd.exe\" \"/c C:\\Applet\\Test.bat {_VirtualMachineName} {CadProcessServiceUrl}\"",
        //                    },
        //                    () => {
        //                        _ErrorMessageAction?.Invoke($"Command has been executed. Name: [{_VirtualMachineName}]");
        //                    },
        //                    () => {
        //                        _ErrorMessageAction?.Invoke($"Command execution has been failed. Name: [{_VirtualMachineName}]");
        //                    }
        //                );
        //        },
        //        () =>
        //        {
        //            _ErrorMessageAction?.Invoke($"Virtual Machine starting has been failed. Name: [{_VirtualMachineName}]");
        //            Wait.Set();

        //        },
        //            _ErrorMessageAction
        //        );
        //    }
        //    else
        //    {
        //        _ErrorMessageAction?.Invoke($"No available virtual machine was found. Name: [{_VirtualMachineName}]");
                
        //    }
        //    Wait.WaitOne();
        //}

        private void CreateDefaultPresets()
        {
            Presets.Add(0, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 50, 200, 500, 1000, 4000, 10000]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 1.0f, 4.0f, 8.0f, 15.0f, 30.0f, 60.0f, 90.0f }),
                LodParameters = "[[14.0, -1, 15, 100.0], [60.0, 50.0, -1, 80.0], [120.0, 100.0, -1, 60.0], [200.0, 200.0, -1, 50.0], [400.0, 400.0, -1, 30.0], [800.0, 800.0, -1, 20.0], [1000.0, 1000.0, 50.0, 10.0]]"
            });

            Presets.Add(1, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 50, 200, 500, 1000, 4000, 10000]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 1.0f, 4.0f, 8.0f, 15.0f, 30.0f, 60.0f, 90.0f }),
                LodParameters = "[[14.0, -1, 15, 100.0], [60.0, 50.0, -1, 80.0], [120.0, 100.0, -1, 60.0], [200.0, 200.0, -1, 50.0], [400.0, 400.0, -1, 30.0], [800.0, 800.0, -1, 20.0], [1000.0, 1000.0, 50.0, 10.0]]"
            });

            Presets.Add(2, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 30.0, 100.0], [12.0, -1, 40.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0 -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(3, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 30.0, 100.0], [12.0, -1, 40.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0 -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(4, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 30.0, 100.0], [12.0, -1, 40.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0 -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(5, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 30.0, 100.0], [12.0, -1, 40.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0 -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });
        }

    }
}