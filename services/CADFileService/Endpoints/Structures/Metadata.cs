/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace CADFileService.Endpoints.Structures
{
    //Note: Custom Procedures microservice uses this class' json structure for decoding the relevant pub/sub messages.
    public class Metadata : IComparable<Metadata>
    {
        public const string METADATA_KEY_PROPERTY = "metadataKey";
        public const string METADATA_VALUES_PROPERTY = "metadataValues";

        [JsonProperty(METADATA_KEY_PROPERTY)]
        public string MetadataKey = "";

        [JsonProperty(METADATA_VALUES_PROPERTY)]
        public List<string> MetadataValues = new List<string>();

        public static readonly string[] MustHaveProperties =
        {
            METADATA_KEY_PROPERTY,
            METADATA_VALUES_PROPERTY
        };

        public static readonly string[] Properties =
        {
            METADATA_KEY_PROPERTY,
            METADATA_VALUES_PROPERTY
        };

        public override bool Equals(object _Other)
        {
            return _Other is Metadata Casted &&
                   MetadataKey == Casted.MetadataKey &&
                   MetadataValues.OrderBy(a => a).SequenceEqual(Casted.MetadataValues.OrderBy(a => a));
        }
        public static bool operator ==(Metadata x, Metadata y)
        {
            return x.Equals(y);
        }
        public static bool operator !=(Metadata x, Metadata y)
        {
            return !x.Equals(y);
        }
        public override int GetHashCode()
        {
            return HashCode.Combine(MetadataKey, MetadataValues);
        }
        public int CompareTo(Metadata _Other)
        {
            return MetadataKey.CompareTo(_Other);
        }
    }
}