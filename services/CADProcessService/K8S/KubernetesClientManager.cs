/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using k8s;

namespace CADProcessService.K8S
{
    public class KubernetesClientManager
    {
        private static string ClusterHost = null;
        private static string ClusterClientKey = null;
        private static string ClusterClientCert = null;

        public static void SetDefaultCredentials(string _ClusterHost, string _ClusterClientKey, string _ClusterClientCert)
        {
            ClusterHost = _ClusterHost;
            ClusterClientKey = _ClusterClientKey;
            ClusterClientCert = _ClusterClientCert;
        }

        public static Kubernetes GetDefaultKubernetesClient()
        {
            return GetKubernetesClient(ClusterHost, ClusterClientKey, ClusterClientCert);
        }

        public static Kubernetes GetKubernetesClient(string _Host, string _ClientCertificateKey = null, string _ClientCertificateData = null)
        {
            KubernetesClientConfiguration Config = new KubernetesClientConfiguration();
            
            Config.Host = _Host;
            if (!_Host.StartsWith("https://"))
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
