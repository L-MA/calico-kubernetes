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

import socket
import logging
import json
import os
import sys
import requests
import re
import sh

from netaddr import IPAddress, IPNetwork, AddrFormatError
from docker import Client
from docker.errors import APIError
import pycalico
from pycalico.netns import PidNamespace,remove_veth
from pycalico.ipam import IPAMClient, SequentialAssignment
from pycalico.datastore_datatypes import Rules, IPPool
from logutils import configure_logger

ETCD_AUTHORITY_ENV = 'ETCD_AUTHORITY'
CALICOCTL_PATH = os.environ.get('CALICOCTL_PATH', '/usr/bin/calicoctl')
KUBE_API_ROOT = os.environ.get('KUBE_API_ROOT',
                               'http://kubernetes-master:8080/api/v1/')
LOG_DIR = '/var/log/calico/kubernetes'

DEFAULT_POLICY = os.environ.get('DEFAULT_POLICY', 'allow')
POLICY_ANNOTATION_KEY = "projectcalico.org/policy"

ORCHESTRATOR_ID = "docker"
HOSTNAME = socket.gethostname()

_log = logging.getLogger(__name__)
_datastore_client = IPAMClient()
_docker_client = Client()
_calicoctl = sh.Command(CALICOCTL_PATH).bake(_env=os.environ)


def calico_kubernetes_cni(args):
    """
    Orchestrate top level function

    :param args: dict of values to pass to other functions (see: validate_args)
    """
    if args['command'] == 'ADD':
        create(args)
    elif args['command'] == 'DEL':
        delete(args)
    else:
        _log.warning('Unknown command: %s', args['command'])


def create(args):
    """"
    Handle a pod-create event.
    Print allocated IP as json to STDOUT

    :param args: dict of values to pass to other functions (see: validate_args)
    """
    container_id = args['container_id']
    namespace = args['namespace']
    pod_name = args['pod_name']
    netns = args['netns']
    interface = args['interface']
    net_name = args['name']
    subnet = args['subnet']

    _log.info('Configuring pod %s' % pod_name)

    endpoint = _create_calico_endpoint(container_id=container_id,
                                       interface=interface,
                                       subnet=subnet)

    _set_profile_on_endpoint(container_id=container_id,
                             namespace=namespace,
                             endpoint=endpoint,
                             pod_name=pod_name,
                             profile_name=net_name)

    dump = json.dumps(
        {
            "ip4": {
                "ip": "%s" % endpoint.ipv4_nets.copy().pop()
            }
        })
    _log.info('Dumping info to kubernetes: %s' % dump)
    print(dump)

    _log.info('Finished Creating pod %s' % pod_name)


def delete(args):
    """
    Cleanup after a pod.

    :param args: dict of values to pass to other functions (see: validate_args)
    """
    container_id = args['container_id']
    net_name = args['name']

    _log.info('Deleting pod %s' % container_id)

    # Remove the profile for the workload.
    _container_remove(hostname=HOSTNAME,
                      orchestrator_id=ORCHESTRATOR_ID,
                      container_id=container_id)

    # Delete profile if only member
    if _datastore_client.profile_exists(net_name) and \
       len(_datastore_client.get_profile_members(net_name)) < 1:
        try:
            _log.info("Profile %s has no members, removing from datastore" % net_name)
            _datastore_client.remove_profile(net_name)
        except:
            _log.error("Cannot remove profile %s: Profile cannot be found." % container_id)
            sys.exit(1)


def _create_calico_endpoint(container_id, interface, subnet):
    """
    Configure the Calico interface for a pod.
    Return Endpoint and IP

    :param container_id (str):
    :param interface (str): iface to use
    :param subnet (str): Subnet to allocate IP to
    :rtype Endpoint: Endpoint created
    """
    _log.info('Configuring Calico networking.')

    try:
        _ = _datastore_client.get_endpoint(hostname=HOSTNAME,
                                          orchestrator_id=ORCHESTRATOR_ID,
                                          workload_id=container_id)
    except KeyError:
        # Calico doesn't know about this container.  Continue.
        pass
    else:
        _log.error("This container has already been configured with Calico Networking.")
        sys.exit(1)

    endpoint = _container_add(hostname=HOSTNAME,
                              orchestrator_id=ORCHESTRATOR_ID,
                              container_id=container_id,
                              interface=interface,
                              subnet=subnet)

    _log.info('Finished configuring network interface')
    return endpoint


def _container_add(hostname, orchestrator_id, container_id, interface, subnet):
    """
    Add a container to Calico networking
    Return Endpoint object and newly allocated IP

    :param hostname (str): Host for enndpoint allocation
    :param orchestrator_id (str): Specifies orchestrator
    :param container_id (str):
    :param interface (str): iface to use
    :param subnet (str): Subnet to allocate IP to
    :rtype Endpoint: Endpoint created
    """
    # Allocate and Assign ip address through _datastore_client
    pool, ip = _assign_to_pool(subnet)

    # Create Endpoint object
    try:
        _log.info("Creating endpoint with IP address %s for container %s",
                  ip, container_id)
        ep = _datastore_client.create_endpoint(HOSTNAME, ORCHESTRATOR_ID,
                                              container_id, [ip])
    except AddrFormatError:
        _log.error("This node is not configured for IPv%d. Unassigning IP "
                   "address %s then exiting." % ip.version, ip)
        _datastore_client.unassign_address(pool, ip)
        sys.exit(1)

    # Obtain the pid of the running container
    pid = _get_container_pid(container_id)

    # Create the veth, move into the container namespace, add the IP and
    # set up the default routes.
    _log.info("Creating the veth with pid %s on interface %s", pid, interface)
    ep.mac = ep.provision_veth(PidNamespace(pid), interface)
    _datastore_client.set_endpoint(ep)

    return ep


def _container_remove(hostname, orchestrator_id, container_id):
    """
    Remove the indicated container on this host from Calico networking

    :param hostname (str): Host for enndpoint allocation
    :param orchestrator_id (str): Specifies orchestrator
    :param container_id (str):
    """
    # Find the endpoint ID. We need this to find any ACL rules
    try:
        endpoint = _datastore_client.get_endpoint(hostname=hostname,
                                                 orchestrator_id=orchestrator_id,
                                                 workload_id=container_id)
    except KeyError:
        _log.error("Container %s doesn't contain any endpoints" % container_id)
        sys.exit(1)

    # Remove any IP address assignments that this endpoint has
    for net in endpoint.ipv4_nets | endpoint.ipv6_nets:
        assert net.size == 1, "Only 1 address allowed per endpoint. Found in network: %s" % net
        _datastore_client.unassign_address(None, net.ip)

    # Remove the endpoint
    remove_veth(endpoint.name)

    # Remove the container from the datastore.
    _datastore_client.remove_workload(hostname=hostname,
                                     orchestrator_id=orchestrator_id,
                                     workload_id=container_id)

    _log.info("Removed Calico interface from %s" % container_id)


def _set_profile_on_endpoint(container_id, namespace, endpoint, pod_name, profile_name):
    """
    Configure the calico profile to the endpoint

    :param endpoint (Endpoint obj): Endpoint to set profile on
    :param profile_name (str): Profile name to add to endpoint
    """
    _log.info('Configuring Pod Profile: %s' % profile_name)

    if not _datastore_client.profile_exists(profile_name):
        _log.info("Creating new profile %s." % (profile_name))
        _datastore_client.create_profile(profile_name)
        _apply_rules(container_id, namespace, pod_name, profile_name)
        _apply_tags(container_id, namespace, pod_name, profile_name)

    # Also set the profile for the workload.
    _datastore_client.set_profiles_on_endpoint(profile_names=[profile_name],
                                              endpoint_id=endpoint.endpoint_id)


def _assign_default_rules(profile_name):
    """
    Generate a new profile rule list and update the _datastore_client
    :param profile_name: The profile to update
    :type profile_name: string
    :return:
    """
    try:
        profile = _datastore_client.get_profile(profile_name)
    except:
        _log.error("Could not apply rules. Profile not found: %s, exiting" % profile_name)
        sys.exit(1)

    rules_dict = {
        "id": profile_name,
        "inbound_rules": [
            {
                "action": "allow",
            },
        ],
        "outbound_rules": [
            {
                "action": "allow",
            },
        ],
    }

    rules_json = json.dumps(rules_dict, indent=2)
    profile_rules = Rules.from_json(rules_json)

    _datastore_client.profile_update_rules(profile)
    _log.info("Finished applying default rules.")


def _assign_to_pool(subnet):
    """
    Take subnet (str), create IP pool in datastore if none exists.
    Allocate next available IP in pool

    :param subnet (str): Subnet to create pool from
    :rtype: (IPPool, IPAddress)
    """
    pool = IPPool(subnet)
    version = IPNetwork(subnet).version
    _datastore_client.add_ip_pool(version, pool)
    candidate = SequentialAssignment().allocate(pool)
    candidate = IPAddress(candidate)

    _log.info("Using Pool %s" % pool)
    _log.info("Using IP %s" % candidate)

    return pool, candidate


def _get_container_info(container_id):
    try:
        info = _docker_client.inspect_container(container_id)
    except APIError as e:
        if e.response.status_code == 404:
            _log.error("Container %s was not found. Exiting.", container_id)
        else:
             _log.error(e.message)
        sys.exit(1)
    return info


def _get_container_pid(container_id):
    return _get_container_info(container_id)["State"]["Pid"]


def _apply_rules(container_id, namespace, pod_name, profile_name):
    """
    Generate a rules for a given profile based on annotations.
    1) Remove Calicoctl default rules
    2) Create new profiles based on annotations, and establish new defaults
    Exceptions:
        If namespace = kube-system (internal kube services), allow all traffic
        If no policy in annotations, allow from pod's Namespace
        Outbound policy should allow all traffic

    :param pod: pod info to parse
    :type pod: dict()
    :return:
    """
    try:
        profile = _datastore_client.get_profile(profile_name)
    except:
        _log.exception("ERROR: Could not apply rules. Profile not found: %s, exiting", profile_name)
        sys.exit(1)

    inbound_rules, outbound_rules = _generate_rules(container_id, namespace, pod_name)

    _log.info("Removing Default Rules")

    # TODO: This method is append-only, not profile replacement, we need to replace calicoctl calls
    #       but necessary functions are not available in pycalico ATM

    # Remove default rules. Assumes 2 in, 1 out.
    try:
        _calicoctl('profile', profile_name, 'rule', 'remove', 'inbound', '--at=2')
        _calicoctl('profile', profile_name, 'rule', 'remove', 'inbound', '--at=1')
        _calicoctl('profile', profile_name, 'rule', 'remove', 'outbound', '--at=1')
    except sh.ErrorReturnCode as e:
        _log.error('Could not delete default rules for profile %s '
                   '(assumed 2 inbound, 1 outbound)\n%s', profile_name, e)

    # Call calicoctl to populate inbound rules
    for rule in inbound_rules:
        _log.info('applying inbound rule \n%s', rule)
        try:
            _calicoctl('profile', profile_name, 'rule', 'add', 'inbound', rule)
        except sh.ErrorReturnCode as e:
            _log.error('Could not apply inbound rule %s.\n%s', rule, e)

    # Call calicoctl to populate outbound rules
    for rule in outbound_rules:
        _log.info('applying outbound rule \n%s' % rule)
        try:
            _calicoctl('profile', profile_name, 'rule', 'add', 'outbound', rule)
        except sh.ErrorReturnCode as e:
            _log.error('Could not apply outbound rule %s.\n%s', rule, e)

    _log.info('Finished applying rules.')


def _apply_tags(container_id, namespace, pod_name, profile_name):
    """
    In addition to Calico's default pod_name tag,
    Add tags generated from Kubernetes Labels and Namespace
        Ex. labels: {key:value} -> tags+= namespace_key_value
    Add tag for namespace
        Ex. namespace: default -> tags+= namespace_default

    :param self.profile_name: The name of the Calico profile.
    :type self.profile_name: string
    :param pod: The config dictionary for the pod being created.
    :type pod: dict
    :return:
    """
    try:
        profile = _datastore_client.get_profile(profile_name)
    except KeyError:
        _log.error('Could not apply tags. Profile %s could not be found. Exiting', profile_name)
        sys.exit(1)

    # Grab namespace and create a tag if it exists.
    ns_tag = _get_namespace_tag(namespace)

    if ns_tag:
        _log.info('Adding tag %s' % ns_tag)
        profile.tags.add(ns_tag)
    else:
        _log.warning('Namespace tag cannot be generated')

    # Create tags from labels
    labels = _get_metadata(namespace, pod_name, 'labels')
    if labels:
        for k, v in labels.iteritems():
            tag = _label_to_tag(k, v)
            _log.info('Adding tag ' + tag)
            profile.tags.add(tag)
    else:
        _log.warning('No labels found in pod %s' % pod_name)

    _datastore_client.profile_update_tags(profile)

    _log.info('Finished applying tags.')


def _generate_rules(container_id, namespace, pod_name):
    """
    Generate Rules takes human readable policy strings in annotations
    and creates argument arrays for calicoctl

    :return two lists of rules(arg lists): inbound list of rules (arg lists)
    outbound list of rules (arg lists)
    """
    ns_tag = _get_namespace_tag(namespace)

    # kube-system services need to be accessed by all namespaces
    if namespace == "kube-system" :
        _log.info("Pod %s belongs to the kube-system namespace - "
                    "allow all inbound and outbound traffic", pod_name)
        return [["allow"]], [["allow"]]

    if namespace and DEFAULT_POLICY == 'ns_isolation':
        inbound_rules = [["allow", "from", "tag", ns_tag]]
        outbound_rules = [["allow"]]
    else:
        inbound_rules = [["allow"]]
        outbound_rules = [["allow"]]

    _log.info("Getting Policy Rules from Annotation of pod %s", pod_name)

    annotations = _get_metadata(namespace, pod_name, "annotations")

    # Find policy block of annotations
    if annotations and POLICY_ANNOTATION_KEY in annotations:
        # Remove Default Rule (Allow Namespace)
        inbound_rules = []
        rules = annotations[POLICY_ANNOTATION_KEY]

        # Rules separated by semicolons
        for rule in rules.split(";"):
            args = rule.split(" ")

            # Labels are declared in the annotations with the format 'label X=Y'
            # These must be converted into format 'tag NAMESPACE_X_Y' to be parsed by calicoctl.
            if 'label' in args:
                # Replace arg 'label' with arg 'tag'
                label_ind = args.index('label')
                args[label_ind] = 'tag'

                # Split given label 'key=value' into components 'key', 'value'
                label = args[label_ind + 1]
                key, value = label.split('=')

                # Compose Calico tag out of key, value components
                tag = _label_to_tag(key, value)
                args[label_ind + 1] = tag

            # Remove empty strings and add to rule list
            args = filter(None, args)
            inbound_rules.append(args)

    return inbound_rules, outbound_rules


def _label_to_tag(ns, label_key, label_value):
    """
    Labels are key-value pairs, tags are single strings. This function handles that translation
    1) Concatenate key and value with '='
    2) Prepend a pod's namespace followed by '/' if available
    3) Escape the generated string so it is Calico compatible

    :param label_key: key to label
    :param label_value: value to given key for a label
    :param namespace: Namespace string, input None if not available
    :param types: (self, string, string, string)
    :return single string tag
    :rtype string
    """
    tag = '%s=%s' % (label_key, label_value)
    tag = '%s/%s' % (ns, tag)
    tag = _escape_chars(tag)
    return tag


def _get_namespace_tag(namespace):
    """
    Pull metadata for namespace and return it and a generated NS tag
    """
    ns_tag = _escape_chars('%s=%s' % ('namespace', ns))
    return ns_tag


def _escape_chars(self, unescaped_string):
    """
    Calico can only handle 3 special chars, '_.-'
    This function uses regex sub to replace SCs with '_'
    """
    # Character to replace symbols
    swap_char = '_'

    # If swap_char is in string, double it.
    unescaped_string = re.sub(swap_char, "%s%s" % (swap_char, swap_char), unescaped_string)

    # Substitute all invalid chars.
    return re.sub('[^a-zA-Z0-9\.\_\-]', swap_char, unescaped_string)


def _get_metadata(namespace, pod_name, key):
    """Return the Metadata[key] of the pod to which the container belongs to"""
    pod_info = _get_pod_config(namespace, pod_name)
    return pod_info["metadata"][key]


def _get_pod_config(namespace, pod_name):
     """
     Get the list of pods from the Kube API server.
     """
     pods = _get_api_path('pods')
     _log.debug('Got pods %s' % pods)

     for pod in pods:
         _log.debug('Processing pod %s', pod)
         if pod['metadata']['namespace'].replace('/', '_') == namespace and \
                         pod['metadata']['name'].replace('/', '_') == pod_name:
             this_pod = pod
             break
         else:
             raise KeyError('Pod %s not found', pod_name)
     _log.debug('Got pod data %s', this_pod)
     return this_pod


def _get_api_path(path):
    """Get a resource from the API specified API path.

    e.g.
    _get_api_path('pods')

    :param path: The relative path to an API endpoint.
    :return: A list of JSON API objects
    :rtype list
    """
    _log.info('Getting API Resource: %s from KUBE_API_ROOT: %s', path, KUBE_API_ROOT)
    bearer_token = _get_api_token()
    session = requests.Session()
    session.headers.update({'Authorization': 'Bearer ' + bearer_token})
    response = session.get(KUBE_API_ROOT + path, verify=False)
    response_body = response.text

    # The response body contains some metadata, and the pods themselves
    # under the 'items' key.
    return json.loads(response_body)['items']


def _get_api_token(self):
    """
    Get the kubelet Bearer token for this node, used for HTTPS auth.
    If no token exists, this method will return an empty string.
    :return: The token.
    :rtype: str
    """
    _log.info('Getting Kubernetes Authorization')

    try:
        with open('/var/lib/kubelet/kubernetes_auth') as f:
            json_string = f.read()
    except IOError as e:
        _log.warning("Failed to open auth_file (%s). Assuming insecure mode", e)
        if self._api_root_secure():
            _log.error("Cannot use insecure mode. API root is set to"
                         "secure (%s). Exiting", KUBE_API_ROOT)
            sys.exit(1)
        else:
            return ""

    _log.info('Got kubernetes_auth: ' + json_string)
    auth_data = json.loads(json_string)
    return auth_data['BearerToken']


def _api_root_secure(self):
    """
    Checks whether the KUBE_API_ROOT is secure or insecure.
    If not an http or https address, exit.

    :return: Boolean: True if secure. False if insecure
    """
    if (KUBE_API_ROOT[:5] == 'https'):
        return True
    elif (KUBE_API_ROOT[:5] == 'http:'):
        return False
    else:
        _log.error('KUBE_API_ROOT is not set correctly (%s). Please specify '
                    'a http or https address. Exiting', KUBE_API_ROOT)
        sys.exit(1)


def validate_args(env, conf):
    """
    Validate and organize environment and stdin args

    ENV =   {
                'CNI_IFNAME': 'eth0',                   req [default: 'eth0']
                'CNI_ARGS': '',
                'CNI_COMMAND': 'ADD',                   req
                'CNI_PATH': '.../.../...',
                'CNI_NETNS': 'netns',                   req [default: 'netns']
                'CNI_CONTAINERID': '1234abcd68',        req
            }
    CONF =  {
                "name": "test",                         req
                "type": "calico",
                "ipam": {
                    "type": "calico-ipam",
                    "subnet": "10.22.0.0/16",           req
                    "routes": [{"dst": "0.0.0.0/0"}],   optional (unsupported)
                    "range-start": ""                   optional (unsupported)
                    "range-end": ""                     optional (unsupported)
                    }
            }
    args = {
                'command': ENV['CNI_COMMAND']
                'interface': ENV['CNI_IFNAME']
                'netns': ENV['CNI_NETNS']
                'name': CONF['name']
                'subnet': CONF['ipam']['subnet']
    }

    :param env (dict): Environment variables from CNI.
    :param conf (dict): STDIN arguments converted to json dict
    :rtype dict:
    """
    _log.debug('Environment: %s' % env)
    _log.debug('Config: %s' % conf)

    args = dict()

    # ENV
    try:
        args['command'] = env['CNI_COMMAND']
    except KeyError:
        _log.error('No CNI_COMMAND in Environment')
        sys.exit(1)
    else:
        if args['command'] not in ["ADD", "DEL"]:
            _log.error('CNI_COMMAND \'%s\' not recognized' % args['command'])

    try:
        args['container_id'] = env['CNI_CONTAINERID']
    except KeyError:
        _log.error('No CNI_CONTAINERID in Environment')
        sys.exit(1)

    try:
        args['namespace'] = env['CNI_ARGS']['K8S_POD_NAMESPACE']
    except KeyError:
        _log.error('No K8S_POD_NAMESPACE in Environment')
        sys.exit(1)

    try:
        args['pod_name'] = env['CNI_ARGS']['K8S_POD_NAME']
    except KeyError:
        _log.error('No K8S_POD_NAME in Environment')
        sys.exit(1)

    try:
        args['interface'] = env['CNI_IFNAME']
    except KeyError:
        _log.exception(
            'No CNI_IFNAME in Environment, using interface \'eth0\'')
        args['interface'] = 'eth0'

    try:
        args['netns'] = env['CNI_NETNS']
    except KeyError:
        _log.exception('No CNI_NETNS in Environment, using \'netns\'')
        args['netns'] = 'netns'

    # CONF
    try:
        args['name'] = conf['name']
    except KeyError:
        _log.error('No Name in Network Config')
        sys.exit(1)

    try:
        args['subnet'] = conf['ipam']['subnet']
    except KeyError:
        _log.error('No Subnet in Network Config')
        sys.exit(1)

    _log.debug('Validated Args: %s' % args)
    return args


if __name__ == '__main__':
    # Setup logger
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    hdlr = logging.FileHandler(filename=LOG_DIR+'/calico-kubernetes-cni.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    _log.addHandler(hdlr)
    _log.setLevel(logging.DEBUG)

    pycalico_logger = logging.getLogger(pycalico.__name__)
    configure_logger(pycalico_logger, logging.DEBUG, False)


    # Environment
    env = os.environ.copy()

    # STDIN args
    conf_raw = ''.join(sys.stdin.readlines()).replace('\n', '')
    conf_json = json.loads(conf_raw).copy()

    # Scrub args
    args = validate_args(env, conf_json)

    # Call plugin
    calico_kubernetes_cni(args)
