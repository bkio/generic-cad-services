using k8s;

namespace CADProcessService.K8S
{
    public class KubernetesClientManager
    {
        private static string GKEHost = null;
        private static string GKEClientKey = null;
        private static string GKEClientCert = null;

        public static void SetDefaultCredentials(string _GKEHost, string _GKEClientKey, string _GKEClientCert)
        {
            GKEHost = _GKEHost;
            GKEClientKey = _GKEClientKey;
            GKEClientCert = _GKEClientCert;
        }

        public static Kubernetes GetDefaultKubernetesClient()
        {
            return GetKubernetesClient(GKEHost, GKEClientKey, GKEClientCert);
        }

        public static Kubernetes GetKubernetesClient(string _Host, string _ClientCertificateKey = null, string _ClientCertificateData = null)
        {
            KubernetesClientConfiguration Config = new KubernetesClientConfiguration();

            Config.Host = $"https://{_Host}";

            Config.SkipTlsVerify = true;

            if (!string.IsNullOrWhiteSpace(_ClientCertificateData))
            {
                Config.ClientCertificateData = _ClientCertificateData;
            }
            if (!string.IsNullOrWhiteSpace(_ClientCertificateKey))
            {
                Config.ClientCertificateKeyData = _ClientCertificateKey;
            }

            return new Kubernetes(Config);
        }
    }
}
