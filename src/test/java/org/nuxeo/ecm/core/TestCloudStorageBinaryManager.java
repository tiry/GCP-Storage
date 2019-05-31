/*
 * (C) Copyright 2019 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Tiry
 */

package org.nuxeo.ecm.core;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.blob.AbstractTestCloudBinaryManager;
import org.nuxeo.ecm.core.storage.gcp.CloudStorageBinaryManager;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.RuntimeFeature;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;

@RunWith(FeaturesRunner.class)
@Features({ RuntimeFeature.class })
public class TestCloudStorageBinaryManager extends AbstractTestCloudBinaryManager<CloudStorageBinaryManager> {

	protected static Map<String, String> PROPERTIES = Collections.emptyMap();

	@BeforeClass
    public static void beforeClass() {

		String gKey = System.getenv("GCP-KEY");
		
        assumeTrue("GCP Key Credentials not set in the environment variables", StringUtils.isNotBlank(gKey));

        PROPERTIES = new HashMap<>();
        PROPERTIES.put(CloudStorageBinaryManager.BUCKET_NAME_PROPERTY, "test-gcpbinarystore-bucket");
        PROPERTIES.put(CloudStorageBinaryManager.PROJECT_ID_PROPERTY, "jx-preprod");        
        PROPERTIES.put(CloudStorageBinaryManager.GOOGLE_APPLICATION_CREDENTIALS, gKey);        
    }
	
	@Override
	protected CloudStorageBinaryManager getBinaryManager() throws IOException {
		CloudStorageBinaryManager binaryManager = new CloudStorageBinaryManager();
        binaryManager.initialize("repo", PROPERTIES);
        return binaryManager;
	}

	@Override
	protected Set<String> listObjects() {
		
		Set<String> ids = new HashSet<>();
		
		Page<Blob> blobs = binaryManager.getBucket().list(BlobListOption.fields(BlobField.ID));
		
		// XXX handle pages!		
		for (Blob blob: blobs.iterateAll()) {
			ids.add(blob.getBlobId().getName());
		}				
		return ids;
	}

}
