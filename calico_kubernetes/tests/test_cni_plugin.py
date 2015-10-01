# Copyright 2015 Metaswitch Networks
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from mock import patch, Mock
from netaddr import IPAddress, IPNetwork
from pycalico.datastore_datatypes import IPPool
import sys
sys.modules['sh'] = Mock()
import calico_kubernetes_cni

CONTAINER_ID = 'ff3afbd1-17ad-499d-b514-72438c009e81'
NETNS_ROOT = '/var/lib/rkt/pods/run'
ORCHESTRATOR_ID = "docker"

ENV = {
    'CNI_IFNAME': 'eth0',
    'CNI_ARGS': {'K8S_POD_NAMESPACE': 'namespace', 'K8S_POD_NAME': 'pod_name'},
    'CNI_COMMAND': 'ADD',
    'CNI_PATH': '.../.../...',
    'CNI_NETNS': 'netns',
    'CNI_CONTAINERID': CONTAINER_ID,
}
CONF = {
    "name": "test",
            "type": "calico",
            "ipam": {
                "type": "host-local",
                "subnet": "10.22.0.0/16",
                "routes": [{"dst": "0.0.0.0/0"}],
                "range-start": "",
                "range-end": "",
            },
}
ARGS = {
    'command': ENV['CNI_COMMAND'],
    'container_id': ENV['CNI_CONTAINERID'],
    'interface': ENV['CNI_IFNAME'],
    'netns': ENV['CNI_NETNS'],
    'name': CONF['name'],
    'subnet': CONF['ipam']['subnet'],
    'namespace': ENV['CNI_ARGS']['K8S_POD_NAMESPACE'],
    'pod_name': ENV['CNI_ARGS']['K8S_POD_NAME']
}



class RktPluginTest(unittest.TestCase):

    @patch('calico_kubernetes_cni.create',
           autospec=True)
    def test_main_ADD(self, m_create):
        ARGS['command'] = 'ADD'
        calico_kubernetes_cni.calico_kubernetes_cni(ARGS)

        m_create.assert_called_once_with(ARGS)

    @patch('calico_kubernetes_cni.delete',
           autospec=True)
    def test_main_DEL(self, m_delete):
        ARGS['command'] = 'DEL'
        calico_kubernetes_cni.calico_kubernetes_cni(ARGS)

        m_delete.assert_called_once_with(ARGS)

    @patch('calico_kubernetes_cni._datastore_client',
           autospec=True)
    @patch('calico_kubernetes_cni._create_calico_endpoint',
           autospec=True)
    @patch('calico_kubernetes_cni._set_profile_on_endpoint',
           autospec=True)
    def test_create(self, m_set_profile, m_create_ep, m_client):

        ip_ = '1.2.3.4/32'
        path_ = '%s/%s/%s' % (NETNS_ROOT, ARGS['container_id'], ARGS['netns'])

        mock_ep = Mock()
        mock_ep.ipv4_nets = set()
        mock_ep.ipv4_nets.add(ip_)
        m_create_ep.return_value = mock_ep

        container_id = ARGS['container_id']
        interface = ARGS['interface']
        subnet = ARGS['subnet']
        namespace = ARGS['namespace']
        pod_name = ARGS['pod_name']
        profile_name =  '%s_%s_%s' % (namespace, pod_name, container_id[:12])

        calico_kubernetes_cni.create(ARGS)

        m_create_ep.assert_called_once_with(container_id=ARGS['container_id'],
                                            interface=ARGS['interface'])
        m_set_profile.assert_called_once_with(container_id=container_id,
                                              namespace=namespace,
                                              endpoint=mock_ep,
                                              pod_name=pod_name,
                                              profile_name=profile_name)

    @patch('calico_kubernetes_cni.HOSTNAME',
           autospec=True)
    @patch('calico_kubernetes_cni._datastore_client',
           autospec=True)
    @patch('calico_kubernetes_cni._container_add', return_value=('ep', 'ip'),
           autospec=True)
    def test_create_calico_endpoint(self, m_con_add, m_client, m_host):
        m_client.get_endpoint.return_value = None
        m_client.get_endpoint.side_effect = KeyError()

        id_, path_ = 'testcontainer', 'path/to/ns'

        calico_kubernetes_cni._create_calico_endpoint(container_id=id_,
                                                      interface=ARGS['interface'])

        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_con_add.assert_called_once_with(hostname=m_host,
                                          orchestrator_id=ORCHESTRATOR_ID,
                                          container_id=id_,
                                          interface=ARGS['interface'])

    @patch("sys.exit",
           autospec=True)
    @patch('calico_kubernetes_cni.HOSTNAME',
           autospec=True)
    @patch('calico_kubernetes_cni._datastore_client',
           autospec=True)
    @patch('calico_kubernetes_cni._container_add', return_value=('ep', 'ip'),
           autospec=True)
    def test_create_calico_endpoint_fail(self, m_con_add, m_client, m_host, m_sys_exit):
        m_client.get_endpoint.return_value = "Endpoint Exists"

        id_, path_ = 'testcontainer', 'path/to/ns'

        calico_kubernetes_cni._create_calico_endpoint(container_id=id_,
                                           interface=ARGS['interface'])

        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_sys_exit.assert_called_once_with(1)

    @patch('calico_kubernetes_cni._assign_ip_address', autospec=True)
    @patch('calico_kubernetes_cni.HOSTNAME', autospec=True)
    @patch('calico_kubernetes_cni._datastore_client', autospec=True)
    @patch('calico_kubernetes_cni._get_container_pid', return_value='12345', autospec=True)
    def test_container_add(self, m_get_container_pid, m_client, m_host, m_assign):
        m_ep = Mock()
        m_client.create_endpoint.return_value = m_ep
        m_ep.provision_veth.return_value = 'macaddress'

        id_= 'testcontainer'
        addr = IPAddress('1.2.3.4')
        m_assign.return_value = addr 

        calico_kubernetes_cni._container_add(hostname=m_host,
                                  orchestrator_id=ORCHESTRATOR_ID,
                                  container_id=id_,
                                  interface=ARGS['interface'])

        m_assign.assert_called_once_with()
        m_client.create_endpoint.assert_called_once_with(
            m_host, ORCHESTRATOR_ID, id_, [addr])
        m_get_container_pid.assert_called_once_with(id_)
        m_ep.provision_veth.assert_called_once()
        m_client.set_endpoint.assert_called_once_with(m_ep)

    @patch.object(calico_kubernetes_cni, "CONFIG", CONF)
    @patch.object(calico_kubernetes_cni, "ENV", ENV)
    @patch('calico_kubernetes_cni.Popen', autospec=True)
    @patch('os.path.isfile', autospec=True)
    def test_call_ipam(self, m_os_isfile, m_popen):
        """
        """
        # Mock out response from IPAM
        stdout = "IPAM plugin stdout"
        stderr = None
        m_popen("").communicate.return_value = (stdout, stderr)
        m_popen.reset_mock()

        # Call method under test
        result = calico_kubernetes_cni._call_ipam_plugin()

        # Assert
        self.assertEquals(result, stdout)

    @patch('calico_kubernetes_cni._unassign_ip_address', autospec=True)
    @patch('calico_kubernetes_cni.HOSTNAME', autospec=True)
    @patch('calico_kubernetes_cni._datastore_client', autospec=True)
    @patch('pycalico.netns', autospec=True)
    def test_container_remove(self, m_netns, m_client, m_host, m_unassign):
        m_ep = Mock()
        m_ep.ipv4_nets = set()
        m_ep.ipv4_nets.add(IPNetwork('1.2.3.4/32'))
        m_ep.ipv6_nets = set()
        m_ep.name = 'endpoint_test'

        m_client.get_endpoint.return_value = m_ep
        id_ = '123'

        calico_kubernetes_cni._container_remove(hostname=m_host,
                                     orchestrator_id=ORCHESTRATOR_ID,
                                     container_id=id_)
        m_unassign.assert_called_once_with()
        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_client.remove_workload.assert_called_once_with(hostname=m_host,
                                                         orchestrator_id=ORCHESTRATOR_ID,
                                                         workload_id=id_)

    @patch('calico_kubernetes_cni._datastore_client',
           autospec=True)
    @patch('calico_kubernetes_cni._generate_rules',
           autospec=True)
    @patch('calico_kubernetes_cni._apply_rules',
           autospec=True)
    @patch('calico_kubernetes_cni._get_annotations_rules',
           autospec=True)
    @patch('calico_kubernetes_cni._apply_tags',
           autospec=True)
    def test_set_profile_on_endpoint(self, m_appy_tags, m_get_annotaitons_rules,
                                     m_apply_rules, m_generate_rules, m_client):
        m_client.profile_exists.return_value = False

        m_ep = Mock()
        m_ep.endpoint_id = '1234'
        m_get_annotaitons_rules.return_value = [], []

        p_name, ip_ = 'profile', '1.2.3.4'
        container_id = ENV['CNI_CONTAINERID']
        namespace = ENV['CNI_ARGS']['K8S_POD_NAMESPACE']
        pod_name = ENV['CNI_ARGS']['K8S_POD_NAME']

        calico_kubernetes_cni._set_profile_on_endpoint(
            container_id=container_id,
            namespace=namespace,
            endpoint=m_ep,
            pod_name=pod_name,
            profile_name=p_name)

        m_client.profile_exists.assert_called_once_with(p_name)
        m_client.set_profiles_on_endpoint.assert_called_once_with(profile_names=[p_name],
                                                                  endpoint_id='1234')

