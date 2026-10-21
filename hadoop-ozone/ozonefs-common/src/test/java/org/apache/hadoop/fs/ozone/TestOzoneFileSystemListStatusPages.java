/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.ListStatusResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The ofs and o3fs listing loops (listStatus and listStatusIterator) stop after a page that OM reports as
 * complete, and keep listing until a page adds nothing when OM does not report it (HDDS-16943). Uses a mock
 * adapter so no cluster is required.
 */
public class TestOzoneFileSystemListStatusPages {

  private static final String DIR = "/vol/bucket/dir";

  private OzoneClientAdapter adapter;

  /** The file system under test, with its listed directory. */
  enum Fs { OFS, O3FS }

  private FileSystem create(Fs type) throws IOException {
    FileSystem fs;
    if (type == Fs.OFS) {
      adapter = mock(BasicRootedOzoneClientAdapterImpl.class);
      fs = new BasicRootedOzoneFileSystem() {
        @Override
        protected OzoneClientAdapter createAdapter(ConfigurationSource conf, String omHost, int omPort) {
          return adapter;
        }
      };
      fs.initialize(URI.create("ofs://om/"), new OzoneConfiguration());
    } else {
      adapter = mock(BasicOzoneClientAdapterImpl.class);
      fs = new BasicOzoneFileSystem() {
        @Override
        protected OzoneClientAdapter createAdapter(ConfigurationSource conf, String bucketStr, String volumeStr,
            String omHost, int omPort) {
          return adapter;
        }
      };
      fs.initialize(URI.create("o3fs://bucket.vol/"), new OzoneConfiguration());
    }
    return fs;
  }

  private static FileStatusAdapter entry(String name) {
    return new FileStatusAdapter(0L, 0L, new Path(DIR + "/" + name), false, (short) 3, 0L, 0L, 0L,
        (short) 0, "user", "group", null, new BlockLocation[0], false, false);
  }

  private static List<FileStatusAdapter> entries(String... names) {
    return Arrays.stream(names).map(TestOzoneFileSystemListStatusPages::entry)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /** Each listing call returns the next page; the listing repeats its start key, as OM does. */
  @SafeVarargs
  private final void stubPages(ListStatusResult<FileStatusAdapter>... pages) throws IOException {
    when(adapter.listStatusPage(anyString(), anyBoolean(), anyString(), anyLong(), any(URI.class),
        any(Path.class), anyString(), anyBoolean()))
        .thenReturn(pages[0], Arrays.copyOfRange(pages, 1, pages.length));
  }

  private void verifyListingCalls(int calls) throws IOException {
    verify(adapter, times(calls)).listStatusPage(anyString(), anyBoolean(), anyString(), anyLong(),
        any(URI.class), any(Path.class), anyString(), anyBoolean());
  }

  private static List<String> names(FileStatus[] statuses) {
    return Arrays.stream(statuses).map(s -> s.getPath().getName()).collect(Collectors.toList());
  }

  private static List<String> names(RemoteIterator<FileStatus> it) throws IOException {
    List<String> names = new ArrayList<>();
    while (it.hasNext()) {
      names.add(it.next().getPath().getName());
    }
    return names;
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusStopsAfterCompletePage(Fs type) throws IOException {
    FileSystem fs = create(type);
    stubPages(ListStatusResult.of(entries("a", "b", "c"), false));

    assertThat(names(fs.listStatus(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(1);
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusContinuesAfterTruncatedPage(Fs type) throws IOException {
    FileSystem fs = create(type);
    // OM capped the first page below the requested size; only the flag says that more entries follow.
    stubPages(ListStatusResult.of(entries("a", "b"), true), ListStatusResult.of(entries("b", "c"), false));

    assertThat(names(fs.listStatus(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(2);
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusWithoutOmReportListsUntilPageAddsNothing(Fs type) throws IOException {
    FileSystem fs = create(type);
    // An older OM: keep listing until a page only repeats the start key.
    stubPages(ListStatusResult.unknown(entries("a", "b", "c")), ListStatusResult.unknown(entries("c")));

    assertThat(names(fs.listStatus(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(2);
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusIteratorStopsAfterCompletePage(Fs type) throws IOException {
    FileSystem fs = create(type);
    stubPages(ListStatusResult.of(entries("a", "b", "c"), false));

    assertThat(names(fs.listStatusIterator(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(1);
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusIteratorContinuesAfterTruncatedPage(Fs type) throws IOException {
    FileSystem fs = create(type);
    stubPages(ListStatusResult.of(entries("a", "b"), true), ListStatusResult.of(entries("b", "c"), false));

    assertThat(names(fs.listStatusIterator(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(2);
  }

  @ParameterizedTest
  @EnumSource(Fs.class)
  public void listStatusIteratorWithoutOmReportListsUntilEmptyPage(Fs type) throws IOException {
    FileSystem fs = create(type);
    stubPages(ListStatusResult.unknown(entries("a", "b", "c")), ListStatusResult.unknown(entries("c")));

    assertThat(names(fs.listStatusIterator(new Path(DIR)))).containsExactly("a", "b", "c");
    verifyListingCalls(2);
  }
}
