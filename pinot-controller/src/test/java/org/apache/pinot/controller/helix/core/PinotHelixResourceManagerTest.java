/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core;

import com.google.common.collect.BiMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.TenantRole;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotHelixResourceManagerTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAX_TIMEOUT_IN_MILLISECOND = 5_000;

  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    config.setEnableLeadControllerResource(true);
    startController(config);

    ControllerRequestBuilderUtil
        .addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES,
            false);
    ControllerRequestBuilderUtil
        .addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES, false,
            BASE_SERVER_ADMIN_PORT);

    // Create server tenant on all Servers
    Tenant serverTenant =
        new Tenant.TenantBuilder(SERVER_TENANT_NAME).setRole(TenantRole.SERVER).setOfflineInstances(NUM_INSTANCES)
            .build();
    _helixResourceManager.createServerTenant(serverTenant);
  }

  @Test
  public void testGetInstanceEndpoints()
      throws InvalidConfigException {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(servers);
    for (int i = 0; i < NUM_INSTANCES; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(BASE_SERVER_ADMIN_PORT + i)));
    }
  }

  @Test
  public void testGetInstanceConfigs()
      throws Exception {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    for (String server : servers) {
      InstanceConfig cachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(server);
      InstanceConfig realInstanceConfig = _helixAdmin.getInstanceConfig(_helixClusterName, server);
      Assert.assertEquals(cachedInstanceConfig, realInstanceConfig);
    }

    ZkClient zkClient = new ZkClient(_helixResourceManager.getHelixZkURL(), CONNECTION_TIMEOUT_IN_MILLISECOND,
        CONNECTION_TIMEOUT_IN_MILLISECOND, new ZNRecordSerializer());

    modifyExistingInstanceConfig(zkClient);
    addAndRemoveNewInstanceConfig(zkClient);

    zkClient.close();
  }

  private void modifyExistingInstanceConfig(ZkClient zkClient)
      throws InterruptedException {
    String instanceName = "Server_localhost_" + new Random().nextInt(NUM_INSTANCES);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(_helixClusterName, instanceName);
    Assert.assertTrue(zkClient.exists(instanceConfigPath));
    ZNRecord znRecord = zkClient.readData(instanceConfigPath, null);

    InstanceConfig cachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
    String originalPort = cachedInstanceConfig.getPort();
    Assert.assertNotNull(originalPort);
    String newPort = Long.toString(System.currentTimeMillis());
    Assert.assertTrue(!newPort.equals(originalPort));

    // Set new port to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), newPort);
    zkClient.writeData(instanceConfigPath, znRecord);

    long maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    InstanceConfig latestCachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
    String latestPort = latestCachedInstanceConfig.getPort();
    while (!newPort.equals(latestPort) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestCachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
      latestPort = latestCachedInstanceConfig.getPort();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Set original port back to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), originalPort);
    zkClient.writeData(instanceConfigPath, znRecord);
  }

  private void addAndRemoveNewInstanceConfig(ZkClient zkClient)
      throws Exception {
    int biggerRandomNumber = NUM_INSTANCES + new Random().nextInt(NUM_INSTANCES);
    String instanceName = "Server_localhost_" + String.valueOf(biggerRandomNumber);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(_helixClusterName, instanceName);
    Assert.assertFalse(zkClient.exists(instanceConfigPath));
    List<String> instances = _helixResourceManager.getAllInstances();
    Assert.assertFalse(instances.contains(instanceName));

    // Add new ZNode.
    ZNRecord znRecord = new ZNRecord(instanceName);
    zkClient.createPersistent(instanceConfigPath, znRecord);

    List<String> latestAllInstances = _helixResourceManager.getAllInstances();
    long maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    while (!latestAllInstances.contains(instanceName) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestAllInstances = _helixResourceManager.getAllInstances();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Remove new ZNode.
    zkClient.delete(instanceConfigPath);

    latestAllInstances = _helixResourceManager.getAllInstances();
    maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    while (latestAllInstances.contains(instanceName) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestAllInstances = _helixResourceManager.getAllInstances();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for removing instance config");
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags()
      throws Exception {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant =
        new Tenant.TenantBuilder(BROKER_TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(3).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Create the table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(3)
            .setBrokerTenant(BROKER_TENANT_NAME).setServerTenant(SERVER_TENANT_NAME).build();
    _helixResourceManager.addTable(tableConfig);

    // Check that the BrokerResource ideal state has 3 Brokers assigned to the table
    IdealState idealState = _helixResourceManager.getHelixAdmin()
        .getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 3);

    // Untag all Brokers assigned to broker tenant
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin
          .removeInstanceTag(_helixClusterName, brokerInstance, TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 0);

    // Create broker tenant on 5 Brokers
    brokerTenant.setNumberOfInstances(5);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 5);

    // Untag all Brokers for other tests
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin
          .removeInstanceTag(_helixClusterName, brokerInstance, TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Delete the table
    _helixResourceManager.deleteOfflineTable(TABLE_NAME);
  }

  @Test
  public void testRetrieveMetadata()
      throws Exception {
    String segmentName = "testSegment";

    // Test retrieving OFFLINE segment ZK metadata
    {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setTableName(OFFLINE_TABLE_NAME);
      offlineSegmentZKMetadata.setSegmentName(segmentName);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
      List<OfflineSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getOfflineSegmentMetadata(OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      OfflineSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      RealtimeSegmentZKMetadata realtimeMetadata = new RealtimeSegmentZKMetadata();
      realtimeMetadata.setTableName(REALTIME_TABLE_NAME);
      realtimeMetadata.setSegmentName(segmentName);
      realtimeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      ZKMetadataProvider.setRealtimeSegmentZKMetadata(_propertyStore, realtimeMetadata);
      List<RealtimeSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getRealtimeSegmentMetadata(REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      RealtimeSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
      Assert.assertEquals(realtimeMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    }
  }

  @Test
  public void testLeadControllerResource() {
    IdealState leadControllerResourceIdealState = _helixResourceManager.getLeadControllerResourceIdealState();
    Assert.assertTrue(leadControllerResourceIdealState.isValid());
    Assert.assertTrue(leadControllerResourceIdealState.isEnabled());
    Assert.assertEquals(leadControllerResourceIdealState.getInstanceGroupTag(),
        CommonConstants.Helix.CONTROLLER_INSTANCE_TYPE);
    Assert.assertEquals(leadControllerResourceIdealState.getNumPartitions(),
        CommonConstants.Helix.DEFAULT_NUMBER_OF_PARTICIPANTS_IN_LEAD_CONTROLLER_RESOURCE);
    Assert.assertEquals(leadControllerResourceIdealState.getReplicas(), "3");
    Assert.assertEquals(leadControllerResourceIdealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    Assert.assertTrue(leadControllerResourceIdealState
        .getInstanceSet(leadControllerResourceIdealState.getPartitionSet().iterator().next()).isEmpty());

    ExternalView leadControllerResourceExternalView = _helixResourceManager.getLeadControllerResourceExternalView();
    for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
      Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
      Assert.assertEquals(stateMap.size(), 1);
      Map.Entry<String, String> entry = stateMap.entrySet().iterator().next();
      Assert.assertEquals(entry.getKey(), CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + "localhost_8998");
      Assert.assertEquals(entry.getValue(), "MASTER");
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
