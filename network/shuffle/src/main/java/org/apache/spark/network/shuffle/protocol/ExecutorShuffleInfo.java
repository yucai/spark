/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/** Contains all configuration necessary for locating the shuffle files of an executor. */
public class ExecutorShuffleInfo implements Encodable {
  /** The base set of local directories that the executor stores its shuffle files in. */
  public final String[] localDirs;
  /** Number of subdirectories created within each localDir. */
  public final int subDirsPerLocalDir;
  /** Shuffle manager (SortShuffleManager or HashShuffleManager) that the executor is using. */
  public final String shuffleManager;
  /** File allocation strategy: hash, hierarchy etc. */
  public final String fileAllocator;
  /** Hierarchy store layers info. */
  public final HierarchyLayerInfo[] availableLayers;

  public ExecutorShuffleInfo(String[] localDirs, int subDirsPerLocalDir, String shuffleManager) {
    this(localDirs, subDirsPerLocalDir, shuffleManager, "HashAllocator", null);
  }

  @JsonCreator
  public ExecutorShuffleInfo(
      @JsonProperty("localDirs") String[] localDirs,
      @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
      @JsonProperty("shuffleManager") String shuffleManager,
      @JsonProperty("fileAllocator") String fileAllocator,
      @JsonProperty("availableLayers") HierarchyLayerInfo[] availableLayers) {
    this.localDirs = localDirs;
    this.subDirsPerLocalDir = subDirsPerLocalDir;
    this.shuffleManager = shuffleManager;
    this.fileAllocator = fileAllocator;
    this.availableLayers = availableLayers;
  }

  @Override
  public int hashCode() {
    return (Objects.hashCode(subDirsPerLocalDir, shuffleManager, fileAllocator) * 41
            + Arrays.hashCode(localDirs)) * 41 + Arrays.hashCode(availableLayers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("localDirs", Arrays.toString(localDirs))
            .add("subDirsPerLocalDir", subDirsPerLocalDir)
            .add("shuffleManager", shuffleManager)
            .add("fileAllocator", fileAllocator)
            .add("availableLayers", Arrays.toString(availableLayers))
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ExecutorShuffleInfo) {
      ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
      return Arrays.equals(localDirs, o.localDirs)
              && Objects.equal(subDirsPerLocalDir, o.subDirsPerLocalDir)
              && Objects.equal(shuffleManager, o.shuffleManager)
              && Objects.equal(fileAllocator, o.fileAllocator)
              && Arrays.equals(availableLayers, o.availableLayers);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    int totalLength = Encoders.StringArrays.encodedLength(localDirs)
            + 4 // int
            + Encoders.Strings.encodedLength(shuffleManager)
            + Encoders.Strings.encodedLength(fileAllocator)
            + 4;
    if (availableLayers != null)
      for (HierarchyLayerInfo layer : availableLayers)
        totalLength += layer.encodedLength();
    return totalLength;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, localDirs);
    buf.writeInt(subDirsPerLocalDir);
    Encoders.Strings.encode(buf, shuffleManager);
    Encoders.Strings.encode(buf, fileAllocator);
    if (availableLayers != null) {
      buf.writeInt(availableLayers.length);
      for (HierarchyLayerInfo layer : availableLayers) {
        layer.encode(buf);
      }
    } else {
      buf.writeInt(0);
    }
  }

  public static ExecutorShuffleInfo decode(ByteBuf buf) {
    String[] localDirs = Encoders.StringArrays.decode(buf);
    int subDirsPerLocalDir = buf.readInt();
    String shuffleManager = Encoders.Strings.decode(buf);
    String fileAllocator = Encoders.Strings.decode(buf);
    HierarchyLayerInfo[] availableLayers = null;
    int numLayers = buf.readInt();
    if (numLayers != 0) {
      availableLayers = new HierarchyLayerInfo[numLayers];
      for (int i = 0; i < numLayers; i++) {
        availableLayers[i] = HierarchyLayerInfo.decode(buf);
      }
    }
    return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager, fileAllocator, availableLayers);
  }
}
