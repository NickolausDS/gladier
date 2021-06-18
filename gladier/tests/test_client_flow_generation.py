from gladier import GladierBaseClient, GladierBaseTool, generate_flow_definition
from globus_automate_client.flows_client import validate_flow_definition


def gen_tool_func():
    pass


@generate_flow_definition
class GeneratedTool(GladierBaseTool):
    """Mock Tool"""
    funcx_functions = [gen_tool_func]


def test_client_flow_generation_simple(logged_in):

    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """Example Docs"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockTool'
        ]

    mc = MyClient()
    flow_def = mc.flow_definition
    assert isinstance(flow_def, dict)
    assert len(flow_def['States']) == 1
    assert flow_def['Comment'] == 'Example Docs'


def test_client_flow_generation_two_tools_two_inst(logged_in):
    """Previously, this caused a KeyError due to overwriting a flow on a tool."""

    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """Example Docs"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockTool',
            GeneratedTool,
        ]

    MyClient()
    mc = MyClient()
    flow_def = mc.flow_definition
    assert isinstance(flow_def, dict)
    assert len(flow_def['States']) == 2
    assert flow_def['Comment'] == 'Example Docs'


def test_client_combine_gen_and_non_gen_flows(logged_in):

    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """My very cool Client"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockTool',
            GeneratedTool,
        ]

    mc = MyClient()
    flow_def = mc.flow_definition
    validate_flow_definition(flow_def)
    assert len(flow_def['States']) == 2


def test_client_tool_with_three_steps(logged_in):

    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """My very cool Client"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockToolThreeStates',
        ]

    mc = MyClient()
    flow_def = mc.flow_definition
    validate_flow_definition(flow_def)
    assert len(flow_def['States']) == 3


def test_client_tool_duplication(logged_in):
    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """My very cool Client"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockTool',
            'gladier.tests.test_data.gladier_mocks.MockTool',
            'gladier.tests.test_data.gladier_mocks.MockTool',
        ]

    mc = MyClient()
    flow_def = mc.flow_definition
    validate_flow_definition(flow_def)
    assert len(flow_def['States']) == 3
    assert set(flow_def['States']) == {'MockFunc', 'MockFunc2', 'MockFunc3'}


def test_client_tool_complex_duplication(logged_in):
    @generate_flow_definition
    class MyClient(GladierBaseClient):
        """My very cool Client"""
        gladier_tools = [
            'gladier.tests.test_data.gladier_mocks.MockToolThreeStates',
            'gladier.tests.test_data.gladier_mocks.MockToolThreeStates',
            'gladier.tests.test_data.gladier_mocks.MockToolThreeStates',
        ]

    mc = MyClient()
    flow_def = mc.flow_definition
    validate_flow_definition(flow_def)
    assert len(flow_def['States']) == 9
    assert set(flow_def['States']) == {
        'StateOne',
        'StateOne2',
        'StateOne3',
        'StateTwo',
        'StateTwo2',
        'StateTwo3',
        'StateThree',
        'StateThree2',
        'StateThree3',
    }
