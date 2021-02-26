/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BCloudServiceUtilities;
using BCommonUtilities;
using k8s.Models;
using ServiceUtilities;
using Newtonsoft.Json;

namespace CADProcessService.K8S
{
    public enum EPodType
    {
        None,
        ProcessPod,
        FileOptimizerPod
    }
    public class BatchProcessingStateService
    {
        private const string FAILED_STATE = "Failed";
        private const string PENDING_STATE = "Pending";
        private const string SUCCESS_STATE = "Succeeded";
        private const string UNKNOWN_STATE = "Unknown";
        private const string RUNNING_STATE = "Running";
        private const string COMPLETED_STATE = "Completed";

        private const string BATCH_NAMESPACE = "cip-batch";

        private const string POD_LIST_KEY = "POD_LIST";
        private const string POD_STATUS_KEY = "POD_STATUS";

        public static BatchProcessingStateService Instance { get; private set; }
        public static K8sObjectManager PodManager { get; private set; }
        private BatchProcessingStateService() { }

        private static readonly HttpClient WebClient = new HttpClient();
        private static IBMemoryServiceInterface MemoryService;

        private static Thread LongRunningThread;

        public static bool Initialize(IBMemoryServiceInterface _MemoryService, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                MemoryService = _MemoryService;
                PodManager = new K8sObjectManager(KubernetesClientManager.GetDefaultKubernetesClient());
                Instance = new BatchProcessingStateService();

                LongRunningThread = new Thread(GeneralStatusCheck);
                LongRunningThread.IsBackground = true;
                LongRunningThread.Start(_ErrorMessageAction);

                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public bool RegisterNewPod(V1Pod Pod, EPodType _PodType, string _BucketName, string _Filename, string _ZipTypeMainAssemblyIfAny, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                if (!UpdatePodStatus(Pod, _PodType, _BucketName, _Filename, _ZipTypeMainAssemblyIfAny))
                {
                    throw new Exception("Could not create/update pod status");
                }
                if (!AddToPodList(Pod.Name()))
                {
                    throw new Exception("Could not add pod to list");
                }
                BTaskWrapper.Run(() =>
                {
                    TrackPodHealthAndStatus(Pod, _PodType, _BucketName, _Filename, _ZipTypeMainAssemblyIfAny, _PodType == EPodType.ProcessPod);
                });
                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to start health check for Pod {Pod.Name()} - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public bool StopProcess(string _PodName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                RemoveFromPodList(_PodName);
                V1Pod Result = PodManager.DeletePod(_PodName, BATCH_NAMESPACE);

                if (Result != null)
                {
                    return true;
                }
                else
                {
                    _ErrorMessageAction?.Invoke("The pod you are trying to delete no longer exists");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to End pod - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public bool PodComplete(string _PodName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                RemoveFromPodList(_PodName);
                V1Pod CurrentState = PodManager.GetPodByNameAndNamespace(_PodName, BATCH_NAMESPACE);

                if (CurrentState != null)
                {
                    UpdatePodStatus(CurrentState, EPodType.None, null, null, null, COMPLETED_STATE);
                    V1Pod Result = PodManager.DeletePod(_PodName, BATCH_NAMESPACE);

                    if (Result != null)
                    {
                        return true;
                    }
                    else
                    {
                        _ErrorMessageAction?.Invoke("The pod you are trying to delete no longer exists");
                        return false;
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke("The pod you are trying to delete no longer exists");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to Delete pod - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }


        private static List<string> GetPodList()
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_LIST_KEY);
            BPrimitiveType ListValue = MemoryService.GetKeyValue(QueryParameters, POD_LIST_KEY, (string Message) => { throw new Exception(Message); });

            if (ListValue != null)
            {
                string ListJson = ListValue.AsString;

                if (ListJson != null)
                {
                    return JsonConvert.DeserializeObject<List<string>>(ListJson);
                }
                else
                {
                    return new List<string>();
                }
            }
            else
            {
                return new List<string>();
            }
        }

        private static bool SetPodList(List<string> _PodList)
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_LIST_KEY);
            return MemoryService.SetKeyValue(QueryParameters, new Tuple<string, BPrimitiveType>[] { new Tuple<string, BPrimitiveType>(POD_LIST_KEY, new BPrimitiveType(JsonConvert.SerializeObject(_PodList))) });
        }

        private static bool AddToPodList(string Item)
        {
            return Locker.LockedAction(POD_LIST_KEY, MemoryService,
            () =>
            {
                List<string> PodList = GetPodList();
                if (!PodList.Contains(Item))
                {
                    PodList.Add(Item);
                }
                return SetPodList(PodList);
            });
        }

        private static bool RemoveFromPodList(string Item)
        {
            return Locker.LockedAction(POD_LIST_KEY, MemoryService,
            () =>
            {
                List<string> PodList = GetPodList();
                PodList.RemoveAll(x => x == Item);
                return SetPodList(PodList);
            });
        }

        private static void GeneralStatusCheck(object _ErrorMessageAction = null)
        {
            Action<string> ErrorMessageAction = null;

            if (_ErrorMessageAction != null)
            {
                try
                {
                    ErrorMessageAction = (Action<string>)_ErrorMessageAction;
                }
                catch (Exception ex)
                {
                    //An invalid parameter was provided but we can still carry on. At this point we don't have logging so default it to a write line method
                    ErrorMessageAction = (Message) => { Console.WriteLine(Message); };
                    ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                }
            }

            try
            {
                GeneralStatusCheck_Internal(ErrorMessageAction);
            }
            catch (Exception ex)
            {
                ErrorMessageAction?.Invoke($"Status checking failed to run. Exiting application - {ex.Message}\n{ex.StackTrace}");
                Environment.Exit(1);
            }
        }

        private static void GeneralStatusCheck_Internal(Action<string> _ErrorMessageAction = null)
        {
            //Keep on trying and don't stop.
            while (true)
            {
                try
                {
                    if (!Locker.LockedAction("POD_STATUS_CHECK", MemoryService, () =>
                     {
                         List<string> PodList = GetPodList();

                         for (int i = 0; i < PodList.Count; ++i)
                         {
                             GetLastPodUpdate(PodList[i], out long LastUpdateLong, out string LastStatus, out EPodType _PodType);

                             if (LastStatus == null)
                             {
                                 //try again on next pass
                                 continue;
                             }

                             DateTime LastUpdate = DateTime.FromBinary(LastUpdateLong);

                             TimeSpan ElapsedTime = DateTime.Now - LastUpdate;

                             //Requeue if unattended for more than a minute and delete if succeeded for more than an hour
                             if (ElapsedTime.TotalSeconds >= 60)
                             {
                                 //Succeeded items are left for an hour for now in-case we need this status afterwards
                                 //Change if necessary
                                 if ((LastStatus == SUCCESS_STATE || LastStatus == COMPLETED_STATE) && ElapsedTime.Hours > 1)
                                 {

                                     //If any one fails here it will try again on next pass
                                     DeleteStatus(PodList[i]);
                                     RemoveFromPodList(PodList[i]);

                                     continue;
                                 }

                                 if (LastStatus != SUCCESS_STATE && LastStatus != COMPLETED_STATE)
                                 {
                                     V1Pod CurrentState = PodManager.GetPodByNameAndNamespace(PodList[i], BATCH_NAMESPACE);
                                     GetPodBucketAndFile(PodList[i], out string BucketName, out string Filename, out string ZipMainAssemblyTypeFile);

                                     if (CurrentState != null && BucketName != null && Filename != null)
                                     {
                                         //If pod is still running but polling has somehow died then reregister it for polling
                                         if (CurrentState.Status.Phase == RUNNING_STATE || CurrentState.Status.Phase == PENDING_STATE)
                                         {
                                             if (!Instance.RegisterNewPod(CurrentState, _PodType, BucketName, Filename, ZipMainAssemblyTypeFile))
                                             {
                                                 //Will try again on next pass
                                                 continue;
                                             }
                                         }
                                     }
                                 }
                             }
                         }
                         return true;
                     }))
                    {
                        throw new Exception("Failed to run Status check");
                    }
                }
                catch (Exception ex)
                {
                    if (!(ex is ThreadAbortException))
                    {
                        //Thread hasn't necessarily stopped at this point so check first before recreating to avoid flooding the system with threads
                        //If thread is still alive then let it loop around and continue as it were
                        //If this thread stops without in a case that is not a full program exit then you lose health check recovery on this service instance
                        if (!LongRunningThread.IsAlive)
                        {
                            LongRunningThread = new Thread(new ParameterizedThreadStart(GeneralStatusCheck))
                            {
                                IsBackground = true
                            };
                            LongRunningThread.Start(_ErrorMessageAction);
                        }
                    }
                }

                Thread.Sleep(10000);
            }
        }

        private bool TrackPodHealthAndStatus(V1Pod Pod, EPodType _PodType, string BucketName, string Filename, string _ZipTypeMainAssemblyIfAny, bool _CheckInternalState = true, Action<string> _ErrorMessageAction = null)
        {
            ManualResetEvent HealthStartProgress = new ManualResetEvent(false);
            int PollSleep = 5000;

            try
            {
                //Wait for pending state to end
                PollTask(PollSleep, 3, () =>
                {
                    V1Pod CurrentState = PodManager.GetPodByNameAndNamespace(Pod.Name(), Pod.Namespace());

                    if (CurrentState == null)
                    {
                        HealthStartProgress.Set();
                        //Pod no longer exists so no point in polling it
                        return true;
                    }

                    if (CurrentState.Status.Phase != PENDING_STATE)
                    {
                        if (UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, null, _ErrorMessageAction))
                        {
                            HealthStartProgress.Set();
                            return true;
                        }
                    }
                    else
                    {
                        //Update and let poll task loop around and check again
                        if (UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, null, _ErrorMessageAction))
                        {
                            return false;
                        }
                    }
                    return false;
                });

                HealthStartProgress.WaitOne();

                //Call endpoint in pod to check if still alive
                PollTask(PollSleep, 3, () =>
                {
                    bool ShouldEnd = false;

                    //ignore lock failures because PollTask will retry
                    Locker.LockedAction(Pod.Name(), MemoryService,
                    () =>
                    {
                        V1Pod CurrentState = PodManager.GetPodByNameAndNamespace(Pod.Name(), Pod.Namespace());

                        if (CurrentState == null)
                        {
                            //Pod no longer exists so no point in polling it
                            //Check if it was completed otherwise register it as Unknown which will queue a restart
                            string LastStatus = GetLastPodStatus(Pod.Name());

                            //If the pod does not exist and was not registered as completed then it is in an unknown state and should be run again
                            if (LastStatus != null && (LastStatus != SUCCESS_STATE || LastStatus != COMPLETED_STATE))
                            {
                                UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, UNKNOWN_STATE);
                            }

                            ShouldEnd = true;
                            return true;
                        }

                        if (CurrentState != null)
                        {
                            if (CurrentState.Status.Phase == SUCCESS_STATE || CurrentState.Status.Phase == COMPLETED_STATE)
                            {
                                if (UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, SUCCESS_STATE))
                                {
                                    //if this fails it will just retry on next run
                                    PodManager.DeletePod(Pod.Name(), Pod.Namespace());
                                }
                                ShouldEnd = true;
                                return true;
                            }
                        }

                        for (int i = 0; i < CurrentState.Status.ContainerStatuses.Count; ++i)
                        {
                            if (CurrentState.Status.ContainerStatuses[i].State == null)
                            {
                                continue;
                            }
                            if (CurrentState.Status.ContainerStatuses[i].State.Terminated == null)
                            {
                                continue;
                            }
                            if (CurrentState.Status.ContainerStatuses[i].State.Terminated.ExitCode != 0)
                            {
                                if (BatchProcessingCreationService.Instance.PodFailure(Pod.Name(), Filename))
                                {
                                    //Don't try to reacreate because it will likely just fail over and over
                                    if (RemoveFromPodList(Pod.Name()))
                                    {
                                        if (UpdatePodStatus(CurrentState, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, FAILED_STATE, _ErrorMessageAction))
                                        {
                                            ShouldEnd = true;
                                        }
                                        try
                                        {
                                            //failing to delete here will mean the pod will be restarted
                                            PodManager.DeletePod(Pod.Name(), Pod.Namespace());
                                            //Pod is now delete so no need to check any other containers
                                            break;
                                        }
                                        catch (Exception ex)
                                        {
                                            _ErrorMessageAction?.Invoke($"{ex.Message}:{ex.StackTrace}");
                                            ShouldEnd = true;
                                        }
                                    }
                                }

                                ShouldEnd = true;

                                return true;
                            }
                        }

                        //Only call in running state
                        if (CurrentState.Status.Phase == RUNNING_STATE)
                        {
                            //if we fail here poll task will try again
                            if (UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny) && _CheckInternalState)
                            {
                                //Assume failure until shown otherwise
                                string InternalState = FAILED_STATE;

                                //Container is still running so call container endpoint to check if still alive
                                using (Task<string> Result = WebClient.GetStringAsync($"http://{CurrentState.Status.PodIP}:8081/healthcheck"))
                                {
                                    Result.Wait();
                                    InternalState = Result.Result;
                                }

                                if (InternalState == FAILED_STATE)
                                {
                                    if (BatchProcessingCreationService.Instance.PodFailure(Pod.Name(), Filename, _ErrorMessageAction))
                                    {
                                        if (UpdatePodStatus(CurrentState, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, FAILED_STATE, _ErrorMessageAction))
                                        {
                                            PodManager.DeletePod(Pod.Name(), Pod.Namespace());
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            //Pod is no longer in a running state so no need to try and poll a container
                            //failing to delete here will mean the pod will be restarted
                            PodManager.DeletePod(Pod.Name(), Pod.Namespace());
                            UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny);

                            ShouldEnd = true;
                            return true;
                        }

                        return true;
                    });

                    return ShouldEnd;
                },
                () =>
                {
                    V1Pod CurrentState = PodManager.GetPodByNameAndNamespace(Pod.Name(), Pod.Namespace());

                    if (CurrentState != null)
                    {
                        //Pod died so delete it (Can't connect to the pod)
                        Locker.LockedAction(Pod.Name(), MemoryService, () =>
                            {
                                if (BatchProcessingCreationService.Instance.PodFailure(Pod.Name(), Filename, _ErrorMessageAction))
                                {
                                    //failing to delete here will mean the pod will be restarted
                                    PodManager.DeletePod(Pod.Name(), Pod.Namespace());
                                    UpdatePodStatus(Pod, _PodType, BucketName, Filename, _ZipTypeMainAssemblyIfAny, FAILED_STATE, _ErrorMessageAction);
                                }
                                return true;
                            });
                    }
                });

                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static void PollTask(int SleepLength, int TryCount, Func<bool> PollAction, System.Action FailedAction = null)
        {
            BTaskWrapper.Run(() =>
            {
                int RetryCount = 0;
                bool Done = false;
                while (!Done)
                {
                    try
                    {
                        Done = PollAction();
                    }
                    catch (Exception)
                    {
                        RetryCount++;
                        if (RetryCount >= TryCount)
                        {
                            FailedAction?.Invoke();
                            break;
                        }
                    }

                    if (Done)
                    {
                        break;
                    }
                    Thread.Sleep(SleepLength);
                }
            });
        }

        private static void MakeQueryParameters(out BMemoryQueryParameters _QueryParameters, string _ItemKey)
        {
            _QueryParameters = new BMemoryQueryParameters();
            _QueryParameters.Domain = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash().ToUpper();
            _QueryParameters.SubDomain = "ServiceUtilities-BATCH";
            _QueryParameters.Identifier = _ItemKey;
        }

        private bool UpdatePodStatus(V1Pod Pod, EPodType _PodType, string _BucketName = null, string _Filename = null, string _ZipMainAssemblyIfAny = null, string ManualStatus = null, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_STATUS_KEY);
                string Status = Pod.Status.Phase;
                string BucketName = _BucketName;
                string Filename = _Filename;
                string ZipMainAssemblyIfAny = "";

                if (ManualStatus != null)
                {
                    Status = ManualStatus;
                }

                if (BucketName == null)
                {
                    BPrimitiveType OldBucketValue = MemoryService.GetKeyValue(QueryParameters, $"{Pod.Name()}-bucket", _ErrorMessageAction);

                    if (OldBucketValue != null)
                    {
                        BucketName = OldBucketValue.AsString;
                    }
                }

                if (Filename == null)
                {
                    BPrimitiveType OldFilenameValue = MemoryService.GetKeyValue(QueryParameters, $"{Pod.Name()}-file", _ErrorMessageAction);

                    if (OldFilenameValue != null)
                    {
                        Filename = OldFilenameValue.AsString;
                    }
                }

                if (_ZipMainAssemblyIfAny == null)
                {
                    BPrimitiveType OldFilenameValue = MemoryService.GetKeyValue(QueryParameters, $"{Pod.Name()}-zip-assembly-type-file", _ErrorMessageAction);

                    if (OldFilenameValue != null)
                    {
                        ZipMainAssemblyIfAny = OldFilenameValue.AsString;
                        if (ZipMainAssemblyIfAny == null)
                        {
                            ZipMainAssemblyIfAny = "";
                        }
                    }
                }

                if (!MemoryService.SetKeyValue(QueryParameters, new Tuple<string, BPrimitiveType>[]
                {
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-pod-name", new BPrimitiveType(Pod.Name())),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-state", new BPrimitiveType(Status)),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-start-time", new BPrimitiveType(Pod.Status.StartTime.GetValueOrDefault().ToBinary())),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-last-update", new BPrimitiveType(DateTime.Now.ToBinary())),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-bucket", new BPrimitiveType(BucketName)),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-file", new BPrimitiveType(Filename)),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-pod-type", new BPrimitiveType((int)_PodType)),
                    new Tuple<string, BPrimitiveType>($"{Pod.Name()}-zip-assembly-type-file", new BPrimitiveType(ZipMainAssemblyIfAny))
                },
                null,
                false))
                {
                    return false;
                }

                if (!MemoryService.SetKeyExpireTime(QueryParameters, TimeSpan.FromDays(1)))
                {
                    return false;
                }
                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static void DeleteStatus(string _PodName)
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_STATUS_KEY);
        }

        private static string GetLastPodStatus(string _PodName)
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_STATUS_KEY);

            BPrimitiveType Value = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-state");

            if (Value != null)
            {
                return Value.AsString;
            }
            else
            {
                return null;
            }
        }
        private static void GetLastPodUpdate(string _PodName, out long _LastUpdate, out string _LastStatus, out EPodType _PodType)
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_STATUS_KEY);

            BPrimitiveType LastUpdateValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-last-update");
            BPrimitiveType StateValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-state");
            BPrimitiveType PodTypeValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-pod-type");

            if (LastUpdateValue != null)
            {
                _LastUpdate = LastUpdateValue.AsInteger;
            }
            else
            {
                _LastUpdate = 0;
            }

            if (StateValue != null)
            {
                _LastStatus = StateValue.AsString;
            }
            else
            {
                _LastStatus = null;
            }

            if (PodTypeValue != null)
            {
                _PodType = (EPodType)PodTypeValue.AsInteger;
            }
            else
            {
                _PodType = EPodType.None;
            }
        }

        public static void GetPodBucketAndFile(string _PodName, out string _BucketName, out string _Filename, out string _ZipMainAssemblyIfAny)
        {
            MakeQueryParameters(out BMemoryQueryParameters QueryParameters, POD_STATUS_KEY);

            BPrimitiveType BucketNameValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-bucket");
            BPrimitiveType FilenameValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-file");
            BPrimitiveType ZipMainAssemblyIfAnyValue = MemoryService.GetKeyValue(QueryParameters, $"{_PodName}-zip-assembly-type-file");


            if (BucketNameValue != null)
            {
                _BucketName = BucketNameValue.AsString;
            }
            else
            {
                _BucketName = null;
            }

            if (FilenameValue != null)
            {
                _Filename = FilenameValue.AsString;
            }
            else
            {
                _Filename = null;
            }

            if (ZipMainAssemblyIfAnyValue != null)
            {
                _ZipMainAssemblyIfAny = ZipMainAssemblyIfAnyValue.AsString;
            }
            else
            {
                _ZipMainAssemblyIfAny = null;
            }
        }
    }
}
