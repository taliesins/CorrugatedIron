﻿// Copyright (c) 2011 - OJ Reeves & Jeremiah Peschka
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Util;
using Newtonsoft.Json;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;

namespace CorrugatedIron.Models
{
    public delegate string SerializeObjectToString<in T>(T theObject);

    public delegate byte[] SerializeObjectToByteArray<in T>(T theObject);

    public delegate T DeserializeObject<out T>(byte[] theObject, string contentType);

    public delegate T ResolveConflict<T>(List<T> conflictedObjects);

    public class RiakObject
    {
        private List<string> _vtags;
        private readonly int _hashCode;
        private readonly Dictionary<string, IntIndex> _intIndexes;
        private readonly Dictionary<string, BinIndex> _binIndexes;

        public string Bucket { get; private set; }
        public string Key { get; private set; }
        public byte[] Value { get; set; }
        public string ContentEncoding { get; set; }
        public string ContentType { get; set; }
        public string CharSet { get; set; }
        public byte[] VectorClock { get; private set; }
        public string VTag { get; private set; }
        public IDictionary<string, string> UserMetaData { get; set; }
        public uint LastModified { get; internal set; }
        public uint LastModifiedUsec { get; internal set; }
        public IList<RiakLink> Links { get; private set; }
        public IList<RiakObject> Siblings { get; set; }

        public IDictionary<string, IntIndex> IntIndexes
        {
            get { return _intIndexes; }
        }

        public IDictionary<string, BinIndex> BinIndexes
        {
            get { return _binIndexes; }
        }

        public DateTime GetLastModified()
        {
            var epochTime = ToEpoch(LastModified, LastModifiedUsec);
            return new DateTime(epochTime, DateTimeKind.Utc);
        }

        public bool HasChanged
        {
            get { return _hashCode != CalculateHashCode(); }
        }

        public List<string> VTags
        {
            get { return _vtags ?? (_vtags = Siblings.Count == 0 ? new List<string> { VTag } : Siblings.Select(s => s.VTag).ToList()); }
        }

        public RiakObject(string bucket, string key)
            : this(bucket, key, null, RiakConstants.Defaults.ContentType)
        {
        }

        public RiakObject(string bucket, string key, string value)
            : this(bucket, key, value, RiakConstants.Defaults.ContentType)
        {
        }

        public RiakObject(string bucket, string key, object value)
            : this(bucket, key, value.ToJson(), RiakConstants.ContentTypes.ApplicationJson)
        {
        }

        public RiakObject(string bucket, string key, string value, string contentType)
            : this(bucket, key, value, contentType, RiakConstants.Defaults.CharSet)
        {
        }

        public RiakObject(string bucket, string key, string value, string contentType, string charSet)
            : this(bucket, key, value.ToRiakString(), contentType, charSet, null)
        {
        }

        public RiakObject(string bucket, string key, string value, string contentType, string charSet, byte[] vectorClock)
            : this(bucket, key, value.ToRiakString(), contentType, charSet, vectorClock)
        {
        }

        public RiakObject(string bucket, string key, byte[] value, string contentType, string charSet, byte[] vectorClock)
        {
            Bucket = bucket;
            Key = key;
            Value = value;
            ContentType = contentType;
            CharSet = charSet;
            VectorClock = vectorClock;
            UserMetaData = new Dictionary<string, string>();
            Links = new List<RiakLink>();
            Siblings = new List<RiakObject>();

            _intIndexes = new Dictionary<string, IntIndex>();
            _binIndexes = new Dictionary<string, BinIndex>();
        }

        public IntIndex IntIndex(string name)
        {
            IntIndex index = null;

            if (!_intIndexes.TryGetValue(name, out index))
            {
                _intIndexes[name] = index = new IntIndex(this, name);
            }

            return index;
        }

        public BinIndex BinIndex(string name)
        {
            BinIndex index = null;

            if (!_binIndexes.TryGetValue(name, out index))
            {
                _binIndexes[name] = index = new BinIndex(this, name);
            }

            return index;
        }

        public void LinkTo(string bucket, string key, string tag)
        {
            Links.Add(new RiakLink(bucket, key, tag));
        }

        public void LinkTo(RiakObjectId riakObjectId, string tag)
        {
            Links.Add(riakObjectId.ToRiakLink(tag));
        }

        public void LinkTo(RiakObject riakObject, string tag)
        {
            Links.Add(riakObject.ToRiakLink(tag));
        }

        public void RemoveLink(string bucket, string key, string tag)
        {
            var link = new RiakLink(bucket, key, tag);
            RemoveLink(link);
        }

        public void RemoveLink(RiakObjectId riakObjectId, string tag)
        {
            var link = new RiakLink(riakObjectId.Bucket, riakObjectId.Key, tag);
            RemoveLink(link);
        }

        public void RemoveLink(RiakObject riakObject, string tag)
        {
            var link = new RiakLink(riakObject.Bucket, riakObject.Key, tag);
            RemoveLink(link);
        }

        public void RemoveLink(RiakLink link)
        {
            Links.Remove(link);
        }

        public void RemoveLinks(RiakObject riakObject)
        {
            RemoveLinks(new RiakObjectId(riakObject.Bucket, riakObject.Key));
        }

        public void RemoveLinks(RiakObjectId riakObjectId)
        {
            var linksToRemove = Links.Where(l => l.Bucket == riakObjectId.Bucket && l.Key == riakObjectId.Key);

            linksToRemove.ForEach(l => Links.Remove(l));
        }

        internal RiakLink ToRiakLink(string tag)
        {
            return new RiakLink(Bucket, Key, tag);
        }

        public RiakObjectId ToRiakObjectId()
        {
            return new RiakObjectId(Bucket, Key);
        }
        
        internal RiakObject(string bucket, string key, RpbContent content, byte[] vectorClock)
        {
            Bucket = bucket;
            Key = key;
            VectorClock = vectorClock;


            Value = content.value;
            VTag = content.vtag.FromRiakString();
            ContentEncoding = content.content_encoding.FromRiakString();
            ContentType = content.content_type.FromRiakString();
            UserMetaData = content.usermeta.ToDictionary(p => p.key.FromRiakString(), p => p.value.FromRiakString());
            Links = content.links.Select(l => new RiakLink(l)).ToList();
            Siblings = new List<RiakObject>();
            LastModified = content.last_mod;
            LastModifiedUsec = content.last_mod_usecs;

            _intIndexes = new Dictionary<string, IntIndex>();
            _binIndexes = new Dictionary<string, BinIndex>();

            foreach (var index in content.indexes)
            {
                var name = index.key.FromRiakString();

                if (name.EndsWith(RiakConstants.IndexSuffix.Integer))
                {
                   IntIndex(name.Remove(name.Length - RiakConstants.IndexSuffix.Integer.Length))
                    .Add(index.value.FromRiakString());
                }
                else
                {
                   BinIndex(name.Remove(name.Length - RiakConstants.IndexSuffix.Binary.Length))
                    .Add(index.value.FromRiakString());
                }
            }

            _hashCode = CalculateHashCode();
        }

        internal RiakObject(string bucket, string key, ICollection<RpbContent> contents, byte[] vectorClock)
            : this(bucket, key, contents.First(), vectorClock)
        {
            if (contents.Count > 1)
            {
                Siblings = contents.Select(c => new RiakObject(bucket, key, c, vectorClock)).ToList();
                _hashCode = CalculateHashCode();
            }
        }

        internal RpbPutReq ToMessage()
        {
            UpdateLastModified();
            var message = new RpbPutReq
            {
                bucket = Bucket.ToRiakString(),
                key = Key.ToRiakString(),
                vclock = VectorClock,
                content = new RpbContent
                {
                    content_type = ContentType.ToRiakString(),
                    value = Value,
                    vtag = VTag.ToRiakString()
                }
            };

            message.content.usermeta.AddRange(UserMetaData.Select(kv => new RpbPair { key = kv.Key.ToRiakString(), value = kv.Value.ToRiakString() }));
            message.content.links.AddRange(Links.Select(l => l.ToMessage()));

            message.content.indexes.AddRange(IntIndexes.Values.SelectMany(i =>
                i.Values.Select(v => new RpbPair { key = i.RiakIndexName.ToRiakString(), value = v.ToString().ToRiakString() })));
            message.content.indexes.AddRange(BinIndexes.Values.SelectMany(i =>
                i.Values.Select(v => new RpbPair { key = i.RiakIndexName.ToRiakString(), value = v.ToRiakString() })));
            
            return message;
        }

        private void UpdateLastModified()
        {
            if (HasChanged)
            {
                var t = DateTime.UtcNow - new DateTime(1970, 1, 1);
                var ms = (ulong)Math.Round(t.TotalMilliseconds);

                LastModified = (uint)(ms / 1000u);
                LastModifiedUsec = (uint)((ms - LastModified * 1000u) * 100u);
            }
        }


        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj))
            {
                return false;
            }

            if(ReferenceEquals(this, obj))
            {
                return true;
            }

            if(obj.GetType() != typeof(RiakObject))
            {
                return false;
            }

            return Equals((RiakObject)obj);
        }

        private long ToEpoch(uint lastModified, uint lastModifiedUsec)
        {
            var epochTime = new DateTime(1970, 1, 1).Ticks;
            epochTime += lastModified * 10000000L;
            epochTime += lastModifiedUsec / 100L;
            return epochTime;
        }

        public bool Equals(RiakObject other)
        {
            if(ReferenceEquals(null, other))
            {
                return false;
            }

            if(ReferenceEquals(this, other))
            {
                return true;
            }

            return Equals(other.Bucket, Bucket)
                && Equals(other.Key, Key)
                && Equals(other.Value, Value)
                && Equals(other.ContentType, ContentType)
                && Equals(other.ContentEncoding, ContentEncoding)
                && Equals(other.CharSet, CharSet)
                && Equals(other.VectorClock, VectorClock)
                && Equals(other.UserMetaData, UserMetaData)
                && Equals(other.BinIndexes, BinIndexes)
                && Equals(other.IntIndexes, IntIndexes)
                && other.LastModified == LastModified
                && other.LastModifiedUsec == LastModifiedUsec
                && Equals(other.Links, Links)
                && Equals(other._vtags, _vtags)
                && other.Links.SequenceEqual(Links)
                && other.UserMetaData.SequenceEqual(UserMetaData)
                && other.BinIndexes.SequenceEqual(BinIndexes)
                && other.IntIndexes.SequenceEqual(IntIndexes);
        }

        public override int GetHashCode()
        {
            return CalculateHashCode();
        }

        /// <summary>
        /// This was moved into its own function that isn't virtual so that it could
        /// be called inside the object's constructor.
        /// </summary>
        /// <returns>The Object's hash code.</returns>
        private int CalculateHashCode()
        {
            unchecked
            {
                var result = (Bucket != null ? Bucket.GetHashCode() : 0);
                result = (result * 397) ^ (Key != null ? Key.GetHashCode() : 0);
                result = (result * 397) ^ (Value != null ? Value.GetHashCode() : 0);
                result = (result * 397) ^ (ContentType != null ? ContentType.GetHashCode() : 0);
                result = (result * 397) ^ (ContentEncoding != null ? ContentEncoding.GetHashCode() : 0);
                result = (result * 397) ^ (CharSet != null ? CharSet.GetHashCode() : 0);
                result = (result * 397) ^ (VectorClock != null ? VectorClock.GetHashCode() : 0);
                result = (result * 397) ^ (UserMetaData != null ? UserMetaData.GetHashCode() : 0);
                result = (result * 397) ^ (BinIndexes != null ? BinIndexes.GetHashCode() : 0);
                result = (result * 397) ^ (IntIndexes != null ? IntIndexes.GetHashCode() : 0);
                result = (result * 397) ^ LastModified.GetHashCode();
                result = (result * 397) ^ LastModifiedUsec.GetHashCode();
                result = (result * 397) ^ (Links != null ? Links.GetHashCode() : 0);
                result = (result * 397) ^ (_vtags != null ? _vtags.GetHashCode() : 0);
                return result;
            }
        }

        public void SetObject<T>(T value, SerializeObjectToString<T> serializeObject)
            where T : class
        {
            if (serializeObject == null)
            {
                throw new ArgumentException("serializeObject cannot be null");
            }

            Value = serializeObject(value).ToRiakString();
        }

        public void SetObject<T>(T value, string contentType, SerializeObjectToString<T> serializeObject)
            where T : class 
        {
            if (string.IsNullOrEmpty(contentType))
            {
                throw new ArgumentException("contentType must be a valid MIME type");
            }

            ContentType = contentType;

            SetObject(value, serializeObject);
        }

        public void SetObject<T>(T value, SerializeObjectToByteArray<T> serializeObject)
        {
            if (serializeObject == null)
            {
                throw new ArgumentException("serializeObject cannot be null");
            }

            Value = serializeObject(value);
        }

        public void SetObject<T>(T value, string contentType, SerializeObjectToByteArray<T> serializeObject)
        {
            if (string.IsNullOrEmpty(contentType))
            {
                throw new ArgumentException("contentType must be a valid MIME type");
            }

            ContentType = contentType;

            SetObject(value, serializeObject);
        }

        // setting content type of SetObject changes content type
        public void SetObject<T>(T value, string contentType = null)
            where T : class
        {
            if(!string.IsNullOrEmpty(contentType))
            {
                ContentType = contentType;
            }

            // check content type
            // save based on content type's deserialization method
            if(ContentType == RiakConstants.ContentTypes.ApplicationJson)
            {
                var sots = new SerializeObjectToString<T>(theObject => theObject.Serialize());
                SetObject(value, ContentType, sots);
                return;
            }

            if(ContentType == RiakConstants.ContentTypes.ProtocolBuffers)
            {
                var soba = new SerializeObjectToByteArray<T>(theObject =>  
                {
                    var ms = new MemoryStream();
                    Serializer.Serialize(ms, value);
                    return ms.ToArray();
                });
                SetObject(value, ContentType, soba);
                return;
            }

            if(ContentType == RiakConstants.ContentTypes.Xml)
            {
                var soba = new SerializeObjectToByteArray<T>(theObject =>
                {
                    var ms = new MemoryStream();
                    var serde = new XmlSerializer(typeof(T));
                    serde.Serialize(ms, value);
                    return ms.ToArray();
                });
                SetObject(value, ContentType, soba);
                return;
            }
            
            if(ContentType.StartsWith("text"))
            {
                Value = value.ToString().ToRiakString();
                return;
            }

            throw new NotSupportedException(string.Format("Your current ContentType ({0}), is not supported.", ContentType));
        }

        public T GetObject<T>(DeserializeObject<T> deserializeObject, ResolveConflict<T> resolveConflict = null)
        {
            if (deserializeObject == null)
            {
                throw new ArgumentException("deserializeObject must not be null");
            }

            if (Siblings.Count > 1 && resolveConflict != null)
            {
                var conflictedObjects = Siblings.Select(s => deserializeObject(s.Value, ContentType)).ToList();

                return resolveConflict(conflictedObjects);
            }

            return deserializeObject(Value, ContentType);
        }

        public T GetObject<T>()
        {
            if(ContentType == RiakConstants.ContentTypes.ApplicationJson)
            {
                var deserializeObject = new DeserializeObject<T>((value, contentType) => JsonConvert.DeserializeObject<T>(Value.FromRiakString()));
                return GetObject(deserializeObject);
            }

            if(ContentType == RiakConstants.ContentTypes.ProtocolBuffers)
            {
                var deserializeObject = new DeserializeObject<T>((value, contentType) =>
                                                                     {
                                                                         var ms = new MemoryStream();
                                                                         ms.Write(value, 0, Value.Length);
                                                                         return Serializer.Deserialize<T>(ms);
                                                                     });
                return GetObject(deserializeObject);
            }

            if(ContentType == RiakConstants.ContentTypes.Xml)
            {
                var deserializeObject = new DeserializeObject<T>((value, contenType) =>
                                                                     {
                                                                         var r = XmlReader.Create(Value.FromRiakString());
                                                                         var serde = new XmlSerializer(typeof (T));
                                                                         return (T) serde.Deserialize(r);
                                                                     }
                    );
                return GetObject(deserializeObject);
            }

            throw new NotSupportedException(string.Format("Your current ContentType ({0}), is not supported.", ContentType));
        }
    }
}