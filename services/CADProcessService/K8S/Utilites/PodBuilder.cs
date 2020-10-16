using k8s.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace CADProcessService.K8S
{
    public class PodBuilder
    {
        private readonly V1Pod Pod;
        private string Namespace = "default";
        public PodBuilder()
        {
            Pod = new V1Pod();
        }

        public V1Pod GetPod()
        {
            Pod.Validate();
            return Pod;
        }

        public string GetNamespace()
        {
            return Namespace;
        }

        public PodBuilder SetName(string _PodName)
        {
            if (Pod.Metadata == null)
            {
                Pod.Metadata = new V1ObjectMeta();
            }
            Pod.Metadata.Name = _PodName;

            return this;
        }

        public PodBuilder SetNamespace(string _Namespace)
        {
            Namespace = _Namespace;

            return this;
        }

        public PodBuilder AddToleration(string _Effect = null, string _Key = null, string _Value = null, string _Operator = null, long? TolerationSeconds = null)
        {
            if (Pod.Spec == null)
            {
                V1PodSpec PodSpec = new V1PodSpec();
            }

            if (Pod.Spec.Tolerations == null)
            {
                Pod.Spec.Tolerations = new List<V1Toleration>();
            }

            Pod.Spec.Tolerations.Add(new V1Toleration(_Effect, _Key, _Operator, TolerationSeconds, _Value));

            return this;
        }

        public PodBuilder AddAnnotation(string _Key, string _Value)
        {
            if (Pod.Metadata == null)
            {
                Pod.Metadata = new V1ObjectMeta();
            }

            if (Pod.Metadata.Annotations == null)
            {
                Pod.Metadata.Annotations = new Dictionary<string, string>();
            }

            Pod.Metadata.Annotations.Add(_Key, _Value);

            return this;
        }

        public PodBuilder AddLabel(string _Key, string _Value)
        {
            if (Pod.Metadata == null)
            {
                Pod.Metadata = new V1ObjectMeta();
            }

            if (Pod.Metadata.Labels == null)
            {
                Pod.Metadata.Labels = new Dictionary<string, string>();
            }

            Pod.Metadata.Labels.Add(_Key, _Value);

            return this;
        }

        public PodBuilder SetRestartPolicy(string _RestartPolicy)
        {
            if (Pod.Spec == null)
            {
                Pod.Spec = new V1PodSpec();
            }

            Pod.Spec.RestartPolicy = _RestartPolicy;

            return this;
        }

        public PodBuilder AddNodeSelector(string _Key, string _Value)
        {
            if (Pod.Spec == null)
            {
                Pod.Spec = new V1PodSpec();
            }

            if (Pod.Spec.NodeSelector == null)
            {
                Pod.Spec.NodeSelector = new Dictionary<string, string>();
            }
            Pod.Spec.NodeSelector.Add(_Key, _Value);

            return this;
        }

        public PodBuilder SetNodeName(string _NodeName)
        {
            if (Pod.Spec == null)
            {
                Pod.Spec = new V1PodSpec();
            }

            Pod.Spec.NodeName = _NodeName;
            
            return this;
        }

        public PodBuilder AddPodAntiAffinity(string _TopologyKey, string _LabelSelector, string _Operator, List<string> _Values )
        {
            if(Pod.Spec.Affinity == null)
            {
                Pod.Spec.Affinity = new V1Affinity();
            }

            if(Pod.Spec.Affinity.PodAntiAffinity == null)
            {
                Pod.Spec.Affinity.PodAntiAffinity = new V1PodAntiAffinity();
            }

            if(Pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == null)
            {
                Pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = new List<V1PodAffinityTerm>();
            }

            V1PodAffinityTerm AffinityTerm = new V1PodAffinityTerm();
            AffinityTerm.TopologyKey = _TopologyKey;
            

            AffinityTerm.LabelSelector = new V1LabelSelector();

            AffinityTerm.LabelSelector.MatchExpressions = new List<V1LabelSelectorRequirement>();
            AffinityTerm.LabelSelector.MatchExpressions.Add(new V1LabelSelectorRequirement(_LabelSelector, _Operator, _Values));

            Pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution.Add(AffinityTerm);

            return this;
        }

        public PodBuilder AddPodAntiAffinityPreference(string _TopologyKey, string _LabelSelector, string _Operator, List<string> _Values)
        {
            if (Pod.Spec.Affinity == null)
            {
                Pod.Spec.Affinity = new V1Affinity();
            }

            if (Pod.Spec.Affinity.PodAntiAffinity == null)
            {
                Pod.Spec.Affinity.PodAntiAffinity = new V1PodAntiAffinity();
            }

            if (Pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution == null)
            {
                Pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = new List<V1WeightedPodAffinityTerm>();
            }

            V1WeightedPodAffinityTerm WeightedAffinity = new V1WeightedPodAffinityTerm();
            WeightedAffinity.Weight = 50;

            V1PodAffinityTerm AffinityTerm = new V1PodAffinityTerm();
            AffinityTerm.TopologyKey = _TopologyKey;

            WeightedAffinity.PodAffinityTerm = AffinityTerm;

            AffinityTerm.LabelSelector = new V1LabelSelector();

            AffinityTerm.LabelSelector.MatchExpressions = new List<V1LabelSelectorRequirement>();
            AffinityTerm.LabelSelector.MatchExpressions.Add(new V1LabelSelectorRequirement(_LabelSelector, _Operator, _Values));

            Pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution.Add(WeightedAffinity);

            return this;
        }

        public PodBuilder AddContainer(string _Name, string _Image, string _ImagePullPolicy = null, int[] _Ports = null, IDictionary<string,string> EnvVars = null, Dictionary<string, List<string>> Command = null)
        {
            if (Pod.Spec == null)
            {
                Pod.Spec = new V1PodSpec();
            }

            if(Pod.Spec.Containers == null)
            {
                Pod.Spec.Containers = new List<V1Container>();
            }

            V1Container Container = new V1Container();
            Container.Name = _Name;
            Container.Image = _Image;
            Container.ImagePullPolicy = _ImagePullPolicy;

            if(_Ports != null && _Ports.Length > 0)
            {
                if(Container.Ports == null)
                {
                    Container.Ports = new List<V1ContainerPort>();
                }

                for(int i = 0; i < _Ports.Length; ++i)
                {
                    V1ContainerPort CurrentPort = new V1ContainerPort(_Ports[i]);
                    Container.Ports.Add(CurrentPort);
                }
            }

            if(EnvVars != null)
            {
                if(Container.Env == null)
                {
                    Container.Env = new List<V1EnvVar>();
                }

                foreach(string CurrentKey in EnvVars.Keys)
                {
                    Container.Env.Add(new V1EnvVar(CurrentKey, EnvVars[CurrentKey]));
                }
            }

            if(Command != null)
            {
                if(Container.Command == null)
                {
                    Container.Command = new List<string>();
                }
                
                foreach(string key in Command.Keys)
                {
                    Container.Command.Add(key);

                    IList<string> Args = Command[key];

                    if(Args != null)
                    {
                        if(Container.Args == null)
                        {
                            Container.Args = new List<string>();
                        }

                        for(int i = 0; i < Args.Count; ++i)
                        {
                            Container.Args.Add(Args[i]);
                        }
                    }
                }
            }
            
            Pod.Spec.Containers.Add(Container);

            return this;
        }
    }
}
