/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using k8s;
using k8s.Models;

namespace CADProcessService.K8S
{
    public class K8sObjectManager
    {
        private readonly Kubernetes KClient;
        private static string WorkerImage;
        private static string CadReaderReaderImage;
        private static string FileOptimizerImage;

        public static void SetImageNames(string _WorkerImage, string _PythonReaderImage, string _UnrealImageName)
        {
            CadReaderReaderImage = _PythonReaderImage;
            WorkerImage = _WorkerImage;
            FileOptimizerImage = _UnrealImageName;
        }

        public K8sObjectManager(Kubernetes _Client)
        {
            KClient = _Client;
        }

        public V1Pod CreateCadReaderPod(string _PodName, string _Namespace, Dictionary<string, string> _PythonWorkerVars, Dictionary<string, string> _FileWorkerVars, Action<string> _ErrorMessageAction = null)
        {
            PodBuilder Builder = new PodBuilder();
            Builder.SetName(_PodName)
                  .SetNamespace(_Namespace)
                  .SetRestartPolicy("Never")
                  .AddLabel("pod-type", "batch-pod")
                  .AddAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "true")
                  .AddNodeSelector("batch-node", "true")
                  .AddToleration("NoSchedule", "reserved-pool", "true", "Equal")
                  .AddPodAntiAffinity("kubernetes.io/hostname", "pod-type", "In", new List<string>() { "batch-pod" })
                  .AddPodAntiAffinityPreference("kubernetes.io/hostname", "pod-type", "In", new List<string>() { "optimizer-pod-ue" })
                  .AddContainer("redis", "redis", "IfNotPresent", new int[] { 6379 }, new Dictionary<string, string>())
                  .AddContainer("cad-reader", CadReaderReaderImage, "IfNotPresent", new int[] { 8080 }, _PythonWorkerVars)
                  .AddContainer("worker", WorkerImage, "IfNotPresent", new int[] { 8081 }, _FileWorkerVars);

            try
            {
                return KClient.CreateNamespacedPod(Builder.GetPod(), Builder.GetNamespace());
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to start Pod {_PodName} : {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public V1Pod CreateFileOptimizerPod(string _PodName, string _Namespace, Dictionary<string, string> _OptimizerEnvVars, Dictionary<string,List<string>> Command, Action<string> _ErrorMessageAction = null)
        {
            PodBuilder Builder = new PodBuilder();
            Builder.SetName(_PodName)
                  .SetNamespace(_Namespace)
                  .SetRestartPolicy("Never")
                  .AddLabel("pod-type", "optimizer-pod")
                  .AddAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "true")
                  .AddNodeSelector("batch-node", "true")
                  .AddToleration("NoSchedule", "reserved-pool", "true", "Equal")
                  .AddPodAntiAffinity("kubernetes.io/hostname", "pod-type", "In", new List<string>() { "optimizer-pod" })
                  .AddContainer("file-optimizer", FileOptimizerImage, "IfNotPresent", new int[] { 8082 }, _OptimizerEnvVars, Command);


            return KClient.CreateNamespacedPod(Builder.GetPod(), Builder.GetNamespace());
        }

        public V1Pod DeletePod(string _PodName, string _Namespace)
        {
            return KClient.DeleteNamespacedPod(_PodName, _Namespace);
        }

        public V1Pod GetPodByNameAndNamespace(string _PodName, string _Namespace)
        {
            try
            {
                V1PodList PodList = KClient.ListNamespacedPod(_Namespace);

                for (int i = 0; i < PodList.Items.Count; ++i)
                {
                    if (PodList.Items[i].Name() == _PodName)
                    {
                        return PodList.Items[i];
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                //Return null if pod could not be found and throw exception when connection error occurs
                throw ex;
            }
        }

        public V1Service GetServiceByNameAndNamespace(string ServiceName, string _Namespace)
        {
            try
            {
                V1ServiceList ServiceList = KClient.ListNamespacedService(_Namespace);

                if (ServiceList != null)
                {
                    for (int i = 0; i < ServiceList.Items.Count; ++i)
                    {
                        if (ServiceList.Items[i].Name() == ServiceName)
                        {
                            return ServiceList.Items[i];
                        }
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                //Return null if pod could not be found and throw exception when connection error occurs
                throw ex;
            }
        }
    }
}
