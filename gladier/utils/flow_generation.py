import logging
import json
from collections import OrderedDict
from gladier.base import GladierBaseTool
from gladier.client import GladierBaseClient
from gladier.utils.flow_modifiers import FlowModifiers
from gladier.utils.name_generation import (
    get_funcx_flow_state_name,
    get_funcx_function_name
)
from gladier.utils.flow_compiler import FlowCompiler


log = logging.getLogger(__name__)


def combine_tool_flows(client: GladierBaseClient, modifiers):
    """
    Combine flow definitions on each of a Gladier Client's **tools** and return
    a single flow definition that runs each state in order from first to last.

    Modifiers can be applied to any of the states within the flow.
    """
    flow_moder = FlowModifiers(client.tools, modifiers)
    flows = [tool.flow_definition for tool in client.tools]
    flow_compiler = FlowCompiler(flows, flow_comment=client.__doc__)
    flow_compiler.compile_flow()

    flow_def = flow_moder.apply_modifiers(flow_compiler.ordered_flow_definition)
    return json.loads(json.dumps(flow_def))


def generate_tool_flow(tool: GladierBaseTool, modifiers):
    """Generate a flow definition for a Gladier Tool based on the defined ``funcx_functions``.
    Accepts modifiers for funcx functions"""

    flow_moder = FlowModifiers([tool], modifiers)

    flow_states = OrderedDict()
    for fx_func in tool.funcx_functions:
        fx_state = generate_funcx_flow_state(fx_func)
        flow_states.update(fx_state)

    flow_def = FlowCompiler.combine_flow_states(flow_states, flow_comment=tool.__doc__)
    flow_def = flow_moder.apply_modifiers(flow_def)
    return json.loads(json.dumps(flow_def))


def generate_funcx_flow_state(funcx_function):

    state_name = get_funcx_flow_state_name(funcx_function)
    tasks = [OrderedDict([
        ('endpoint.$', '$.input.funcx_endpoint_compute'),
        ('func.$', f'$.input.{get_funcx_function_name(funcx_function)}'),
        ('payload.$', '$.input'),
    ])]
    flow_state = OrderedDict([
        ('Comment', funcx_function.__doc__),
        ('Type', 'Action'),
        ('ActionUrl', 'https://api.funcx.org/automate'),
        ('ActionScope', 'https://auth.globus.org/scopes/'
                        'facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2'),
        ('ExceptionOnActionFailure', False),
        ('Parameters', OrderedDict(tasks=tasks)),
        ('ResultPath', f'$.{state_name}'),
        ('WaitTime', 300),
    ])
    return OrderedDict([(state_name, flow_state)])
