/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using BCloudServiceUtilities;
using BCommonUtilities;
using BServiceUtilities;
using CADProcessService.Endpoints.Controllers;
using CADProcessService.Endpoints.Structures;
using k8s.Models;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADProcessService.K8S
{
    public class BatchProcessingCreationService
    {
        private const string SERVICE_NAMESPACE = "cip";
        private const string BATCH_NAMESPACE = "cip-batch";

        private const int URL_VALIDITY_MINUTES = 1440;
        private const string UPLOAD_CONTENT_TYPE = "application/octet-stream";
        private readonly K8sObjectManager K8sManager;
        public static BatchProcessingCreationService Instance { get; private set; }
        private static IBFileServiceInterface FileService;
        private static IBDatabaseServiceInterface DatabaseService;

        private static string CadProcessUrl;

        private static ManualResetEvent WaitInit = new ManualResetEvent(false);

        private static Dictionary<string, string> FileWorkerEnvironmentVariables = new Dictionary<string, string>();
        private static string DeploymentBuildNumber;
        private static string DeploymentBranchName;

        private BatchProcessingCreationService()
        {
            K8sManager = new K8sObjectManager(KubernetesClientManager.GetDefaultKubernetesClient());
        }

        public static void Initialize(
            IBDatabaseServiceInterface _DatabaseService,
            IBFileServiceInterface _FileService,
            string _DeploymentBranchName,
            string _DeploymentBuildNumber,
            string _CadProcessServiceName,
            Dictionary<string, string> _FileWorkerEnvironmentVariables,
            System.Action _InitFailedAction,
            Action<string> _ErrorMessageAction = null)
        {
            FileWorkerEnvironmentVariables = _FileWorkerEnvironmentVariables;
            DeploymentBranchName = _DeploymentBranchName;
            DeploymentBuildNumber = _DeploymentBuildNumber;


            FileService = _FileService;
            DatabaseService = _DatabaseService;
            Instance = new BatchProcessingCreationService();

            string CadProcessServiceName = _CadProcessServiceName;

            BTaskWrapper.Run(() =>
            {

                while (true)
                {
                    try
                    {
                        V1Service CadProcessService = Instance.K8sManager.GetServiceByNameAndNamespace(CadProcessServiceName, SERVICE_NAMESPACE);

                        if (CadProcessService != null
                        && CadProcessService.Status != null
                        && CadProcessService.Status.LoadBalancer != null
                        && CadProcessService.Status.LoadBalancer.Ingress != null
                        && CadProcessService.Status.LoadBalancer.Ingress.Any()

                        && CadProcessService.Spec != null
                        && CadProcessService.Spec.Ports != null
                        && CadProcessService.Spec.Ports.Any()
                        && !string.IsNullOrWhiteSpace(CadProcessService.Status.LoadBalancer.Ingress.First().Ip))
                        {
                            CadProcessUrl = $"http://{CadProcessService.Status.LoadBalancer.Ingress.First().Ip}:{CadProcessService.Spec.Ports.First().Port}/";
                            WaitInit.Set();
                            break;
                        }
                        else
                        {
                            Thread.Sleep(1000);
                        }
                    }
                    catch (Exception ex)
                    {
                        _ErrorMessageAction?.Invoke($"Failed to initialize Batch process environment: {ex.Message}\n{ex.StackTrace}");
                        //If we fail at this point then it means the cluster master endpoint is unavailable or there is no ingress which means batch process can't system can't initialize.
                        //This can happen instantly or 5 minutes after the program started depending on if an ingress is still being created and how long it takes so decide what to do in provided action
                        _InitFailedAction.Invoke();
                    }
                }
            });
        }

        public bool StartFileOptimizer(string _BucketName, string _FileName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                //Do not try to create a pod if initialization hasn't finished yet
                WaitInit.WaitOne();

                string PodName = GetFileOptimizerPodName(_BucketName, _FileName);
                V1Pod CheckExistingPod = K8sManager.GetPodByNameAndNamespace(PodName, BATCH_NAMESPACE);

                if (CheckExistingPod != null)
                {
                    return true;
                }

                Dictionary<string, string> OptimizerEnvVars = new Dictionary<string, string>();

                OptimizerEnvVars.Add("CAD_PROCESS_PARAMETER_REQUEST_URL", $"{CadProcessUrl}3d/process/internal/get_file_optimizer_parameters/{PodName}");

                Dictionary<string, List<string>> Command = new Dictionary<string, List<string>>();
                List<string> Args = new List<string>();

                Args.Add($"{OptimizerEnvVars["CAD_PROCESS_PARAMETER_REQUEST_URL"]}");
                
                Command.Add("/tmp/project/run_commandlet.sh", Args);

                V1Pod CreatedPod = K8sManager.CreateFileOptimizerPod(PodName, BATCH_NAMESPACE, OptimizerEnvVars, Command, _ErrorMessageAction);

                if (CreatedPod == null)
                {
                    _ErrorMessageAction?.Invoke("Could not create unreal optimizer pod");
                    return false;
                }


                if (!BatchProcessingStateService.Instance.RegisterNewPod(CreatedPod, EPodType.FileOptimizerPod, _BucketName, _FileName, null, _ErrorMessageAction))
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
        public Dictionary<string, string> GetOptimizerEnvVars(string _BucketName, string _Filename, string PodName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                //Do not try to create a pod if initialization hasn't finished yet
                WaitInit.WaitOne();

                Dictionary<string, string> EnvVars = new Dictionary<string, string>();

                string RelativeBucketFile = _Filename.TrimStart("raw/").TrimEnd(".zip");

                EnvVars.Add("CAD_PROCESS_UPLOAD_REQUEST_URL", $"{CadProcessUrl}3d/process/internal/get_signed_upload_url_for_unreal_file/{PodName}");

                if (!CreateSignedDownloadUrlEnvVars(_BucketName, RelativeBucketFile, EnvVars, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create Download Urls");
                    return null;
                }

                return EnvVars;

            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public bool StartBatchProcess(string _BucketName, string _FileName, string _ZipTypeMainAssemblyIfAny, out string _PodName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                //Do not try to create a pod if initialization hasn't finished yet otherwise the pod won't have a proper notify URL
                WaitInit.WaitOne();

                string PodName = GetPixyzPodName(_BucketName, _FileName);
                _PodName = PodName;
                V1Pod CheckExistingPod = K8sManager.GetPodByNameAndNamespace(PodName, BATCH_NAMESPACE);

                //If a pod already exists then re-register it.
                //If pod already exists in a failure state then it will be handled.
                if (CheckExistingPod != null)
                {
                    return BatchProcessingStateService.Instance.RegisterNewPod(CheckExistingPod, EPodType.ProcessPod, _BucketName, _FileName, _ZipTypeMainAssemblyIfAny, _ErrorMessageAction);
                }

                if (!FileService.CreateSignedURLForDownload(out string SignedDownloadUrl, _BucketName, _FileName, 10))
                {
                    return false;
                }


                Dictionary<string, string> PythonWorkerVars = new Dictionary<string, string>();
                PythonWorkerVars.Add("SIGNED_FILE_URL", SignedDownloadUrl);
                if (!string.IsNullOrWhiteSpace(_ZipTypeMainAssemblyIfAny))
                {
                    PythonWorkerVars.Add("ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY", _ZipTypeMainAssemblyIfAny);
                }

                Dictionary<string, string> FileWorkerVars = new Dictionary<string, string>();
                SetFileWorkerEnvVars(PodName, FileWorkerVars);

                //Extarct part of path that can be reused
                string RelativeBucketFile = _FileName.TrimStart("raw/").TrimEnd(".zip");

                if (!CreateSignedUploadUrlEnvVars(_BucketName, FileWorkerVars, RelativeBucketFile, _ErrorMessageAction))
                {
                    return false;
                }

                V1Pod CreatedPod = K8sManager.CreateCadReaderPod(PodName, BATCH_NAMESPACE, PythonWorkerVars, FileWorkerVars, _ErrorMessageAction);

                if (CreatedPod == null)
                {
                    _ErrorMessageAction?.Invoke("Could not create batch pod");
                    return false;
                }

                if (!BatchProcessingStateService.Instance.RegisterNewPod(CreatedPod, EPodType.ProcessPod, _BucketName, _FileName, _ZipTypeMainAssemblyIfAny, _ErrorMessageAction))
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to start batch process : {ex.Message}\n{ex.StackTrace}");
                _PodName = null;
                return false;
            }
        }

        private static bool CreateSignedUploadUrlEnvVars(string _BucketName, Dictionary<string, string> _EnvVarsDictionary, string _RelativeBucketFile, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_HIERARCHY_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.HIERARCHY_CF]}",
                    UPLOAD_CONTENT_TYPE, URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_HIERARCHY_RAF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_RAF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.HIERARCHY_RAF]}",
                    UPLOAD_CONTENT_TYPE,
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_GEOMETRY_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.GEOMETRY_CF]}",
                    UPLOAD_CONTENT_TYPE,
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_GEOMETRY_RAF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_RAF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.GEOMETRY_RAF]}",
                    UPLOAD_CONTENT_TYPE,
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_METADATA_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.METADATA_CF]}",
                    UPLOAD_CONTENT_TYPE,
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }
                if (!FileService.CreateSignedURLForUpload(
                    out string SignedUploadUrl_UPLOAD_METADATA_RAF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_RAF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.METADATA_RAF]}",
                    UPLOAD_CONTENT_TYPE,
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }

                _EnvVarsDictionary.Add("UPLOAD_HIERARCHY_CF", SignedUploadUrl_UPLOAD_HIERARCHY_CF);
                _EnvVarsDictionary.Add("UPLOAD_HIERARCHY_RAF", SignedUploadUrl_UPLOAD_HIERARCHY_RAF);
                _EnvVarsDictionary.Add("UPLOAD_GEOMETRY_CF", SignedUploadUrl_UPLOAD_GEOMETRY_CF);
                _EnvVarsDictionary.Add("UPLOAD_GEOMETRY_RAF", SignedUploadUrl_UPLOAD_GEOMETRY_RAF);
                _EnvVarsDictionary.Add("UPLOAD_METADATA_CF", SignedUploadUrl_UPLOAD_METADATA_CF);
                _EnvVarsDictionary.Add("UPLOAD_METADATA_RAF", SignedUploadUrl_UPLOAD_METADATA_RAF);

                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to create Signed URLs - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static bool CreateSignedDownloadUrlEnvVars(string _BucketName, string _RelativeBucketFile, Dictionary<string, string> _EnvVarsDictionary, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                if (!FileService.CreateSignedURLForDownload(
                    out string SignedDownloadUrl_UPLOAD_HIERARCHY_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.HIERARCHY_CF]}",
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for HIERARCHY_RAF");
                    return false;
                }

                if (!FileService.CreateSignedURLForDownload(
                    out string SignedDownloadUrl_UPLOAD_GEOMETRY_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.GEOMETRY_CF]}",
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for GEOMETRY_CF");
                    return false;
                }

                if (!FileService.CreateSignedURLForDownload(
                    out string SignedDownloadUrl_UPLOAD_METADATA_CF,
                    _BucketName,
                    $"{Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_CF]}{_RelativeBucketFile}.{Constants.ProcessedFileType_Extension_Map[EProcessedFileType.METADATA_CF]}",
                    URL_VALIDITY_MINUTES,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to create signed URL for METADATA_CF");
                    return false;
                }


                _EnvVarsDictionary.Add("DOWNLOAD_HIERARCHY_CF", SignedDownloadUrl_UPLOAD_HIERARCHY_CF);
                _EnvVarsDictionary.Add("DOWNLOAD_GEOMETRY_CF", SignedDownloadUrl_UPLOAD_GEOMETRY_CF);
                _EnvVarsDictionary.Add("DOWNLOAD_METADATA_CF", SignedDownloadUrl_UPLOAD_METADATA_CF);

                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to create Signed URLs - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static void SetFileWorkerEnvVars(string PodName, Dictionary<string, string> FileWorkerVars)
        {
            FileWorkerVars.Add("PORT", "8081");
            FileWorkerVars.Add("PROGRAM_ID", "CadWorkerProcess");

            foreach (var key in FileWorkerEnvironmentVariables.Keys)
            {
                FileWorkerVars.Add(key, FileWorkerEnvironmentVariables[key]);
            }
            FileWorkerVars.Add("DEPLOYMENT_BRANCH_NAME", DeploymentBranchName);
            FileWorkerVars.Add("DEPLOYMENT_BUILD_NUMBER", DeploymentBuildNumber);

            FileWorkerVars.Add("REDIS_ENDPOINT", "127.0.0.1");
            FileWorkerVars.Add("REDIS_PORT", "6379");
            FileWorkerVars.Add("REDIS_PASSWORD", "N/A");

            FileWorkerVars.Add("CAD_PROCESS_NOTIFY_URL", $"{CadProcessUrl}3d/process/internal/job-complete/{PodName}");
        }

        public bool StopBatchProcess(string BucketName, string FileName, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                string PodName = GetPixyzPodName(BucketName, FileName);
                string UPodName = GetFileOptimizerPodName(BucketName, FileName);

                //We can live with this failing because unreal optimizer will exit and get cleaned out automatically
                TryKillPod(UPodName, BATCH_NAMESPACE, _ErrorMessageAction);

                return BatchProcessingStateService.Instance.StopProcess(PodName, _ErrorMessageAction);
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to stop batch process - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public bool NotifyPodSucceded(string _PodName, Action<string> _ErrorMessageAction = null)
        {
            return BatchProcessingStateService.Instance.PodComplete(_PodName, _ErrorMessageAction);
        }

        public void GetBucketAndFile(string _PodName, out string _Bucket, out string _Filename)
        {
            BatchProcessingStateService.GetPodBucketAndFile(_PodName, out _Bucket, out _Filename, out string _);
        }

        private static bool GetModelDetailsFromFilePath(
            string _Filename, 
            out string _ModelId, 
            out int _RevisionIndex, 
            Action<string> _ErrorMessageAction = null)
        {
            try
            {
                //Expecting format raw/b7192e4a29de7c3be8f1fbdd12913886/0/0/file.zip"
                string[] Parts = _Filename.Split('/');
                _ModelId = Parts[1];
                _RevisionIndex = int.Parse(Parts[2]);
                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to extract model info from [{_Filename}] - {ex.Message}\n{ex.StackTrace}");
                _ModelId = null;
                _RevisionIndex = -1;
                return false;
            }
        }

        public bool TryKillPod(string _Podname, string _Namespace, Action<string> _ErrorMessageAction = null)
        {
            try
            {
                K8sManager.DeletePod(_Podname, _Namespace);
                return true;
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"{ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public bool PodFailure(string _Podname, string _Filename = null, Action<string> _ErrorMessageAction = null)
        {
            _ErrorMessageAction?.Invoke($"Pod failure reported : {_Podname}");
            try
            {
                string Filename = _Filename;
                if (Filename == null)
                {
                    GetBucketAndFile(_Podname, out string _, out Filename);

                    if (string.IsNullOrWhiteSpace(Filename))
                    {
                        _ErrorMessageAction?.Invoke("Could not find pod details");
                        return false;
                    }
                }

                if (GetModelDetailsFromFilePath(Filename, out string _ModelId, out int _RevisionIndex, _ErrorMessageAction))
                {
                    //if we fail then you have a broken model on a broken path
                    Controller_BatchProcess.Get().BroadcastBatchProcessAction(new Action_BatchProcessFailed()
                    {
                        ModelID = _ModelId,
                        RevisionIndex = _RevisionIndex
                    },
                    _ErrorMessageAction);
                }

                var NewDBEntry = new FileConversionDBEntry()
                {
                    ConversionStatus = (int)EInternalProcessStage.ProcessFailed
                };

                string ConversionId = WebUtility.UrlEncode(Filename);

                return DatabaseService.UpdateItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(ConversionId),
                    JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                    out JObject _, EBReturnItemBehaviour.ReturnAllNew,
                    null,
                    _ErrorMessageAction);
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to handle Pod failure - {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        private static string GetPixyzPodName(string _Bucket, string _Filename)
        {
            int HashCode = $"{_Bucket}{_Filename}".GetHashCode();
            return $"batch-pod-{HashCode}";
        }

        private static string GetFileOptimizerPodName(string _Bucket, string _Filename)
        {
            int HashCode = $"{_Bucket}{_Filename}".GetHashCode();
            return $"file-optimizer-{HashCode}";
        }

    }
}
