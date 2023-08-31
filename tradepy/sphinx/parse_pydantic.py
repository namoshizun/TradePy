from inspect import isclass
import yaml
from pydantic import BaseModel
from docutils import nodes
from sphinx.application import Sphinx


def generate_yaml_tree(model):
    def _recurse_attrs(cls):
        attrs = {}
        for attr_name, attr_value in cls.__annotations__.items():
            if isclass(attr_value) and issubclass(attr_value, BaseModel):
                attrs[attr_name] = _recurse_attrs(attr_value)
            else:
                attrs[attr_name] = str(attr_value)
        return attrs

    return _recurse_attrs(model)


def yaml_tree_directive(
    name,
    arguments,
    options,
    content,
    lineno,
    content_offset,
    block_text,
    state,
    state_machine,
):
    model_name = arguments[0]
    module_name = arguments[1]

    try:
        model = getattr(__import__(module_name, fromlist=[model_name]), model_name)
    except ImportError:
        raise ValueError(f"Unable to import module {module_name} or model {model_name}")

    yaml_tree = generate_yaml_tree(model)
    yaml_tree_str = yaml.dump(yaml_tree, default_flow_style=False)
    print(yaml_tree_str)

    node = nodes.literal_block(yaml_tree_str, yaml_tree_str)
    return [node]


def setup(app: Sphinx):
    app.add_directive("pydantic-yaml-tree", yaml_tree_directive)
    return {"version": "1.0", "parallel_read_safe": True}
